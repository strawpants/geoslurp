# This file is part of geoslurp.
# geoslurp is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# geoslurp is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with Frommle; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from geoslurp.dataset import DataSet
from geoslurp.datapull.thredds import Crawler,ThreddsFilter, getAttrib
from geoalchemy2.types import Geography
from geoalchemy2.elements import WKBElement
from sqlalchemy import Column,Integer,String, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
from queue import Queue
from threading import Thread
import logging
import os
# To do:  etract meta information with a threadpool
#from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.ext.declarative import declarative_base

#create a custom exception which describes netcdf datasets with dimensions of zero length
class ZeroDimException(Exception):
    pass

# A declarative base which can be used to create database tables

OceanObsTBase=declarative_base(metadata=MetaData(schema='oceanobs'))

# Setup the postgres table

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

class ArgoTable(OceanObsTBase):
    """Defines the Argo PostgreSQL table"""
    __tablename__='argo'
    id=Column(Integer,primary_key=True)
    datacenter=Column(String)
    lastupdate=Column(TIMESTAMP)
    tprofile=Column(TIMESTAMP)
    tlocation=Column(TIMESTAMP)
    wmoid=Column(Integer)
    cycle=Column(Integer)
    uri=Column(String, index=True)
    mode=Column(String,index=True)
    profnr=Column(Integer)
    ascending=Column(Boolean)
    geom=Column(geoPointtype)
    data=Column(JSONB)

def ncStr(ncelem):
    """extracts a utf-8 encoded string from a  netcdf character variable and strips trailing junk"""
    return b"".join(ncelem).decode('utf-8').strip("\0")

def argoMetaExtractor(uri,cachedir=False):
    """Extract meta information as a drictionary from  an argo floats
    Each registered profile gets a separate entry"""

    meta=[]
    try:

        ncArgo=ncDset(uri.opendap)
        url=uri.opendap
        #test whether the dataset has zero dimensions (fails for opendap)
        ncreplace=None
        for ky,val in ncArgo.dimensions.items():
            if val.size == 0:
                if ky == 'N_HISTORY' and cachedir:
                    # HACK: allright many argo profiles suffer from a zero dimension N_HISTORY
                    # which cannot currently be loaded using opendap
                    # in that case we download the file an reopen the netcdf file locally
                    urilocal,succ=uri.download(cachedir,True)
                    url=urilocal.url # this now points to a local file
                    ncreplace=ncDset(url)
                    break
                else:
                    # we can't cope with this
                    raise ZeroDimException('Netcdf dimension '+ky+' is zero for '+url)
        if ncreplace:
            ncArgo.close()
            ncArgo=ncreplace

        logging.info("Extracting meta info from: %s"%(url))

        # Get reference time
        t0=datetime.strptime(b"".join(ncArgo["REFERENCE_DATE_TIME"][:]).decode("utf-8"),"%Y%m%d%H%M%S")
        for iprof in range(ncArgo.dimensions["N_PROF"].size):
            # geographical location
            geoLoc=ogr.Geometry(ogr.wkbPoint)
            geoLoc.AddPoint(float(ncArgo["LONGITUDE"][iprof]), float(ncArgo["LATITUDE"][iprof]))
            # time point
            tprof=t0+timedelta(days=float(ncArgo["JULD"][iprof].data))
            tloc=t0+timedelta(days=float(ncArgo["JULD_LOCATION"][iprof].data))
            cycle=int(ncArgo["CYCLE_NUMBER"][iprof].data)
            datacenter=ncStr(ncArgo["DATA_CENTRE"][iprof])
            direction=bool(ncArgo["DIRECTION"][iprof].data == b"A")
            wmoid=int(ncStr(ncArgo["PLATFORM_NUMBER"][iprof]))
            mode=ncStr(ncArgo["DATA_MODE"][:])[iprof]
            meta.append({"datacenter":datacenter,"lastupdate":uri.lastmod, "tprofile":tprof,
                         "tlocation":tloc, "wmoid":wmoid, "cycle":cycle , "uri":url,
                         "mode":mode, "profnr":iprof, "ascending":direction,
                         "geom":WKBElement(geoLoc.ExportToWkb(),srid=4326,extended=True),"data":{}})
    except ZeroDimException as e:
        raise RuntimeWarning(str(e)+", skipping")
    except Exception as e:
        raise RuntimeWarning("Cannot extract meta information from "+ url+ str(e))

    return meta


class Argo(DataSet):
    """Argo table"""
    __version__=(0,0,0)
    def __init__(self,scheme):
        super().__init__(scheme)

        # Create table if it doesn't exist
        # import pdb;pdb.set_trace()
        OceanObsTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)
        self._uriqueue=Queue(maxsize=100)
        self._killUpdate=False
        #set Argo mirrors and datacenters
        #add thredds catalogs when non-existent
        if 'thredds' not in self._inventData:
            catalogs= ["http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS/", "https://data.nodc.noaa.gov/thredds/catalog/argo/gdac/"]
            self._inventData['thredds']=[]
            filt=ThreddsFilter("catalogRef")
            followfilt=ThreddsFilter("dataset")
            for cat in catalogs:
                crwl=Crawler(cat+'catalog.xml',filter=filt,followfilter=followfilt)
                servdict=crwl.services._asdict()
                servdict['centers']=[getAttrib(el,'title') for el in crwl.xmlitems()]
                self._inventData['thredds'].append(servdict)

    def pull(self):
        """Stub because the actual pulling takes place in a separate thread in the register function"""
        pass

    def register(self,center=None,mirror=0):
        """Extracts metadata from the float and registers it in the database
            :param center: specifies the processing center to screen (default takes all available)
                currently avalaible are: aoml, bodc, coriolis, csio, csiro, incois, jma, kma, kordi, meds, nmdis
            :param mirror: use mirror 0: https://tds0.ifremer.fr (default) or mirror 1: https://data.nodc.noaa.gov
        """

        #first retrieve a list of catalogrefs  (i.e. datacenters)
        # make a list of all available centers
        if center:
            #only consider a specific center
            if center not in self._inventData['thredds'][mirror]['centers']:
                raise RuntimeError('Datacenter not found')
            centers=[center]
        else:
            centers=self._inventData['thredds'][mirror]['centers']

        for cent in centers:
            #loop over all processing centers
            logging.info("Getting catalog of processing center %s"%(cent))
            #determine center catalog url
            catalogurl=self._inventData['thredds'][mirror]['baseurl']+self._inventData['thredds'][mirror]['catalog']+cent+'/catalog.xml'
            filt = ThreddsFilter("dataset", attr="urlPath", regex=".*profiles.*")

            # let's start a thread which will start quering the threddsserver and queues jobs
            t=Thread(target=self.pullWorker, kwargs={"conn":Crawler(catalogurl, filter=filt)})
            t.start()

            #create a dedicated database session
            ses=self.scheme.db.Session()
            i=0
            while not self._killUpdate:
                try:
                    uri=self._uriqueue.get()
                    if uri == None:
                        # done
                        break
                    # Check whether an entry already exists inthe database which is up to date
                    try:
                        qResult=ses.query(ArgoTable).filter(ArgoTable.uri.like('%'+uri.suburl+'%')).first()
                        if qResult.lastupdate >= uri.lastmod:
                            logging.info("No Update needed, skipping %s"%(uri.suburl))
                            self._uriqueue.task_done()
                            continue
                        else:
                            #delete the entries which need updating
                            ses.query(ArgoTable).filter(ArgoTable.uri.like('%'+uri.suburl+'%')).delete()
                            # ses.delete(qResult)
                    except:
                        # Fine no entries found
                        pass

                    for metadict in argoMetaExtractor(uri,self.cacheDir(os.path.dirname(uri.suburl))):
                        entry=ArgoTable(**metadict)
                        ses.add(entry)
                        if i > 10:
                            # commit every so many rows
                            ses.commit()
                            i=0
                        else:
                            i+=1
                    self._uriqueue.task_done()
                except RuntimeWarning as e:
                    logging.warning(e)
                    logging.warning("Skipping record")
                    continue
                except Exception as e2:
                    # let the threddscrawler come to a gracefull halt
                   self.stopUpdate()

            ses.commit()
            #also update the inventory
            self._inventData["lastupdate"]=datetime.now().isoformat()
            self._inventData["version"]=self.__version__
            if cent not in self._inventData['thredds'][mirror]['centers']:
                self._inventData['centers'].append(cent)


            self.updateInvent()
            t.join()

    def purge(self):
        pass

    def stopUpdate(self):
        logging.error("Stopping update")
        self._killUpdate=True

    def pullWorker(self,conn):
        """ Pulls valid opendap URI's from a thredds server and queue them"""

        try:
            for uri in conn.uris():
                self._uriqueue.put(uri)
        except Exception as e:
            self._uriqueue.put(None)
            raise RuntimeError("Pulling of Argo URI's stopped")
        #signal the end of the queue by adding a none
        self._uriqueue.put(None)

