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
from geoslurp.datapull import ThreddsConnector,ThreddsFilter,getDate
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
from geoslurp.config import Log
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

def argoMetaExtractor(uri):
    """Extract meta information as a drictionary from  an argo floats
    Each registered profile gets a separate entry"""

    meta=[]
    try:
        print("extracting meta info from: %s"%(uri),file=Log)
        ncArgo=ncDset(uri)
        
        #test whether the dataset has zero dimensions (fails for opendap)
        for ky,val in ncArgo.dimensions.items():
            if val.size == 0:
                raise ZeroDimException('Netcdf dimension '+ky+' is zero for '+uri)

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
            meta.append({"datacenter":datacenter,"lastupdate":datetime.now(), "tprofile":tprof,
                         "tlocation":tloc, "wmoid":wmoid, "cycle":cycle , "uri":uri,
                         "mode":mode, "profnr":iprof, "ascending":direction,
                         "geom":WKBElement(geoLoc.ExportToWkb(),srid=4326,extended=True),"data":{}})
    except ZeroDimException as e:
        raise RuntimeWarning(str(e)+", skipping")
    except Exception as e:
        raise RuntimeWarning("Cannot extract meta information from "+ uri+ str(e))

    return meta


class Argo(DataSet):
    """Argo table"""
    __version__=(0,0,0)
    def __init__(self,scheme):
        super().__init__(scheme)

        # Create table if it doesn't exist
        # import pdb;pdb.set_trace()
        # OceanObsTBase.metadata.create_all(self.scheme.db.dbeng, tables=[ArgoTable],checkfirst=True)
        OceanObsTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)
        self._uriqueue=Queue(maxsize=10)
        self._killUpdate=False

    def pull(self):
        """Stub because the actual pulling takes place in a separate thread in the register function"""
        pass

    def register(self,center=None,mirror=0):
        """Extracts metadata from the float and registers it in the database
            :param center: specifies the processing center to screen (default takes all available)
                currently avalaible are: aoml, bodc, coriolis, csio, csiro, incois, jma, kma, kordi, meds, nmdis
            :param mirror: use mirror 0: https://tds0.ifremer.fr (default) or mirror 1: https://data.nodc.noaa.gov
        """
        t=Thread(target=self.pullWorker,kwargs={"center":center,"mirror":mirror})
        t.start()

        #create a database session
        ses=self.scheme.db.Session()
        i=0
        while not self._killUpdate:
            try:
                lastmod,uri=self._uriqueue.get()
                if uri == None:
                    # done
                    break
                # Check whether an entry already exists inthe database which is up to date
                try:
                    qResult=ses.query(ArgoTable).filter(ArgoTable.uri == uri).first()
                    if qResult.lastupdate > lastmod:
                        print("No Update needed, skipping %s"%(uri))
                        self._uriqueue.task_done()
                        continue
                    else:
                        #delete the entries which need updating
                        ses.query(ArgoTable).filter(ArgoTable.uri == uri).delete()
                except:
                    # Fine no entries found
                    pass

                for metadict in argoMetaExtractor(uri):
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
                print(e)
                print("Skipping record",file=Log)
                continue
            except Exception as e2:
                # let the threddscrawler come to a gracefull halt
               self.stopUpdate()

        ses.commit()
        #also update the inventory
        self._inventData={"lastupdate":datetime.now().isoformat(),"version":self.__version__}
        self.updateInvent()
        t.join()
    def purge(self):
        pass

    def stopUpdate(self):
        print("Stopping update",file=Log)
        self._killUpdate=True

    def pullWorker(self,center,mirror=0):
        """ Pulls valid opendap URI's from a thredds server and queue them"""

        filt=ThreddsFilter("dataset",attr="urlPath",regex=".*profiles.*")
        mirrors=["http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS", "https://data.nodc.noaa.gov/thredds/catalog/argo/gdac/"]
        rootcatalog=mirrors[mirror]
        if center != None:
            rootcatalog+="/"+center
        rootcatalog+='/catalog.xml'
        try:
            conn=ThreddsConnector(rootcatalog,filter=filt)
            for ds in conn.items():
                if self._killUpdate:
                    break
                uri=conn.rooturl+"/"+conn.services.opendap+'/'+ds.attrib['urlPath']

                # also extract the last modification date
                lastmod=getDate(ds)
                print("Queuing %s"%(uri),file=Log)
                self._uriqueue.put((lastmod,uri))
        except Exception as e:
            # put in a None to signal the workers in register() that the queue ended
            self._uriqueue.put((None,None))
            raise RuntimeError("Pulling of Argo URI's stopped")


        # put in a None to signal the workers in register() that the queue ended
        self._uriqueue.put((None, None))
