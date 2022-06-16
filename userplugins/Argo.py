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

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.datapull.ftp import Crawler as ftpCrawler
from geoslurp.datapull.ftp import Uri as ftpUri
from geoslurp.datapull import findFiles
from geoslurp.datapull import UriFile
from geoslurp.tools.netcdftools import ncStr
from geoalchemy2.types import Geography
from geoalchemy2.elements import WKBElement
from sqlalchemy import Column,Integer,String, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
from queue import Queue
import gzip as gz
from geoslurp.config.slurplogger import slurplogger
from geoslurp.config.catalogue import geoslurpCatalogue
import os
import re
import numpy as np
# To do:  etract meta information with a threadpool
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.ext.declarative import declarative_base

scheme='oceanobs'

class ArgoftpCrawler(ftpCrawler):
    """Adapted ftpcrawler class to get speedier (concurrent) downloads for argo files
    Takes advantage of the argo index files"""
    def uris(self):
        """This creates a list of _prof.nc files without having to list subdirectories"""

        buf=ftpUri(self.rooturl+"ar_index_global_meta.txt.gz").buffer()
        regex=re.compile('^([a-z]+/[0-9]+/)([0-9]+_meta.nc),[0-9]+,.{2},([0-9]+)')
        for ln in gz.decompress(buf.getvalue()).splitlines():
            mtch=regex.search(ln.decode('utf-8'))
            if not re.match(self.pattern,ln.decode('utf-8')):
                continue
            if mtch:
                subdir=mtch.group(1)
                fname=mtch.group(2).replace('meta','prof')
                t=datetime.strptime(mtch.group(3),"%Y%m%d%H%M%S")
                yield ftpUri(os.path.join(self.rooturl,'dac',subdir,fname),lastmod=t,subdirs=subdir)


#create a custom exception which describes netcdf datasets with dimensions of zero length
class ZeroDimException(Exception):
    pass

# A declarative base which can be used to create database tables

OceanObsTBase=declarative_base(metadata=MetaData(schema=scheme))

# Setup the postgres table

geoMpointType = Geography(geometry_type="MULTIPOINTZ", srid='4326', spatial_index=True,dimension=3,from_text="ST_GeogfromWKB")

class ArgoTable(OceanObsTBase):
    """Defines the Argo PostgreSQL table"""
    __tablename__='argo2'
    id=Column(Integer,primary_key=True)
    wmoid=Column(String)
    uri=Column(String, index=True,unique=True)
    datacenter=Column(String)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    mode=Column(ARRAY(String))
    ascend=Column(ARRAY(Boolean))
    tlocation=Column(ARRAY(TIMESTAMP))
    cycle=Column(ARRAY(Integer))
    iprof=Column(ARRAY(Integer))
    geom=Column(geoMpointType)


def argoMetaExtractor(uri,cachedir=False):
    """Extract meta information (tracks, etc) as a dictionary from an argo prof file  floats"""

    try:
        url=uri.url
        ncArgo=ncDset(url)

        #check for minimum amount of profiles
        minProfs=3
        if ncArgo.dimensions['N_PROF'].size < minProfs:
            slurplogger().info("amount of profiles in %s is less then %d, skipping"%(url,minProfs))
            return {}



        slurplogger().info("Extracting meta info from: %s"%(url))

        # Get reference time
        t0=datetime.strptime(ncStr(ncArgo["REFERENCE_DATE_TIME"][:]),"%Y%m%d%H%M%S")

        #get the start end end time


        #wmoid's should be the same for all entries, so take the first one
        wmoid=int(ncStr(ncArgo["PLATFORM_NUMBER"][0]))

        datacenter=ncStr(ncArgo["DATA_CENTRE"][0])

        #get modes for each profile
        mode=np.array([x for x in ncStr(ncArgo['DATA_MODE'])])

        #get cycles for each profile
        cycle=[int(x) for x in ncArgo['CYCLE_NUMBER'][:]]

        #
        tlocation=[]
        # which profile is ascending ?
        ascend=ncArgo['DIRECTION'][:]== b"A"

        geoMpoints=ogr.Geometry(ogr.wkbMultiPoint)
        iprof=[]
        for i,(t,lon,lat) in enumerate(zip(ncArgo["JULD_LOCATION"][:],ncArgo["LONGITUDE"][:],ncArgo["LATITUDE"][:])):
            if lon > 180:
                #make sure longitude goes from -180 to 180
                lon-=360
            #we don't want nan positions or timetags in the database
            if np.ma.is_masked(lon) or np.ma.is_masked(lat) or np.ma.is_masked(t):
                continue

            tdt=t0+timedelta(days=float(t))
            tlocation.append(tdt)
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(float(lon),float(lat),0)
            geoMpoints.AddGeometry(point)
            iprof.append(i)

        if not tlocation:
            #return an empty dictionary when no valid profiles have been found
            return {}

        tstart=np.min(tlocation)
        tend=np.max(tlocation)

        meta={"wmoid":wmoid,"uri":url,"datacenter":datacenter,"lastupdate":uri.lastmod,"tstart":tstart,"tend":tend,
              "mode":mode,"ascend":ascend,"tlocation":tlocation,"cycle":cycle,"iprof":iprof,
              "geom":geoMpoints.ExportToWkb()}
    except Exception as e:
        raise RuntimeWarning("Cannot extract meta information from "+ url+ str(e))

    return meta


class Argo2(DataSet):
    """Argo table"""
    version=(0,0,0)
    table=ArgoTable
    scheme=scheme
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.updated=[]
        # Create table if it doesn't exist
        # import pdb;pdb.set_trace()
        OceanObsTBase.metadata.create_all(self.db.dbeng, checkfirst=True)
        self._uriqueue=Queue(maxsize=300)
        self._killUpdate=False
        self.thrd=None

    def pull(self,center=None,mirror=0):
        """ Pulls the combined \*_prof.nc files from the ftp server
        :param center (string): only pull data from a specific datacenter
        :param mirror (0 or 1): use ifremer (0) or usgodae (1) mirror
        """
        ftpmirrors=["ftp://ftp.ifremer.fr/ifremer/argo/","ftp://usgodae.org/pub/outgoing/argo/"]

        #since crawling thought the ftp directories takes relatively much time we're going to speed this up using the dedicated ArgoftpCrawler
        if center:
            ftpcrwl=ArgoftpCrawler(ftpmirrors[mirror],center)
        else:
            ftpcrwl=ArgoftpCrawler(ftpmirrors[mirror])
        self.updated=ftpcrwl.parallelDownload(self.dataDir(),check=True,maxconn=10,continueonError=True)


    def register(self,center=None):
        """register downloaded commbined prof files"""
        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            slurplogger().info("Building file list..")
            files=[UriFile(file) for file in findFiles(self.dataDir(),'.*nc',self._dbinvent.lastupdate)]

        if len(files) == 0:
            slurplogger().info("Argo: No new files found since last update")
            return

        filesnew=self.retainnewUris(files)
        if len(filesnew) == 0:
            slurplogger().info("Argo: No database update needed")
            return
        #loop over files
        for uri in filesnew:
            if center and not re.search(center,uri.url):
                continue
            meta=argoMetaExtractor(uri)
            if meta:
                self.addEntry(meta)



        self.updateInvent()


    # def halt(self):
        # slurplogger().error("Stopping update")
        # self._killUpdate=True
        # # indicate a done task n the queue in order to allow the pullWorker thread to stop gracefully
        # #empty eue
        # while not self._uriqueue.empty():
            # self._uriqueue.get()
            # self._uriqueue.task_done()
        # #also synchronize inventory info (e.g. resume
        # self.updateInvent(False)
        # raise RuntimeWarning("Argo dataset processing stopped")

    # def pullWorker(self,conn):
        # """ Pulls valid opendap URI's from a thredds server and queue them"""

        # for uri in conn.uris():
            # slurplogger().info("queuing %s",uri.url)
            # self._uriqueue.put(uri)
            # if self._killUpdate:
                # slurplogger().warning("Pulling of Argo URI's stopped")
                # return
        # #signal the end of the queue by adding a none
        # self._uriqueue.put(None)

geoslurpCatalogue.addDataset(Argo2)
