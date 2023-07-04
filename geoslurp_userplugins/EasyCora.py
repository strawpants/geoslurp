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

# Authors Roelof Rietbroek (r.rietbroek@utwente.nl) and Alisa Yakhontova, 2021

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.datapull.ftp import Crawler as ftpCrawler
import tarfile
from geoslurp.tools.tarsafe import tar_safe_extractall
from geoslurp.tools.netcdftools import ncStr
from geoslurp.datapull import findFiles
from geoslurp.datapull import UriFile
from geoalchemy2.types import Geometry,Geography
from geoalchemy2.elements import WKBElement
from sqlalchemy import Column,Integer,String, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import MetaData
from netCDF4 import Dataset as ncDset
from netCDF4 import num2date

from osgeo import ogr
from datetime import datetime,timedelta
from queue import Queue
from geoslurp.config.slurplogger import slurplogger
from geoslurp.config.catalogue import geoslurpCatalogue
import os
from pathlib import Path
import re
import numpy as np
# To do:  etract meta information with a threadpool
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.ext.declarative import declarative_base

scheme='oceanobs'

# A declarative base which can be used to create database tables

OceanObsTBase=declarative_base(metadata=MetaData(schema=scheme))

# Setup the postgres table

geoMpointType = Geography(geometry_type="MULTIPOINTZ", srid='4326', spatial_index=True,dimension=3,from_text="ST_GeogfromWKB")


class CoraTable(OceanObsTBase):
    """Defines the Cora PostgreSQL table"""
    __tablename__='easycora'
    id=Column(Integer,primary_key=True)
    wmoid=Column(String)
    uri=Column(String, index=True,unique=True)
    datacenter=Column(String)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    mode=Column(ARRAY(String))
    #ascend=Column(ARRAY(Boolean))
    tlocation=Column(ARRAY(TIMESTAMP))
    cycle=Column(ARRAY(Integer))
    iprof=Column(ARRAY(Integer))
    geom=Column(geoMpointType)



def coraMetaExtractor(uri):
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
        
        # t0=datetime.strptime(ncStr(ncArgo["REFERENCE_DATE_TIME"][:]),"%Y%m%d%H%M%S"\cc)
        t0=num2date(0.0, units=ncArgo['JULD'].units,only_use_cftime_datetimes=False)

        #get the start end end time


        #wmoid's should be the same for all entries, so take the first one
        # wmoid=int(ncStr(ncArgo["PLATFORM_NUMBER"][0]))
        wmoid=[]
        
        # this is the file type: mooring, profiler, ...
        datacenter=url.split('_')[-2]+'_'+url.split('_')[-1][0:2]
        

        #get modes for each profile
        mode=np.array([x for x in ncStr(ncArgo['DATA_MODE'])])

        #get cycles for each profile
        cycle=[int(x) for x in ncArgo['CYCLE_NUMBER'][:]]

        #
        tlocation=[]
        # which profile is ascending ?
        # ascend=ncArgo['DIRECTION'][:]== b"A"

        geoMpoints=ogr.Geometry(ogr.wkbMultiPoint)
        iprof=[]
        for i,(t,lon,lat) in enumerate(zip(ncArgo["JULD"][:],ncArgo["LONGITUDE"][:],ncArgo["LATITUDE"][:])):
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
        meta={"uri":url,"lastupdate":uri.lastmod,"datacenter":datacenter,"tstart":tstart,"tend":tend,
              "mode":mode,"tlocation":tlocation,"cycle":cycle,"iprof":iprof,
              "geom":geoMpoints.ExportToIsoWkb()}
    except Exception as e:
        raise RuntimeWarning("Cannot extract meta information from "+ url+ str(e))

    return meta


class EasyCora(DataSet):
    """Cora table"""
    version=(0,0,0)
    table=CoraTable
    scheme=scheme
    def __init__(self,dbcon):
        
        super().__init__(dbcon)
        
    def pull(self,pattern='.*'):
        """ Pulls the Easy CORA file from the copernicus FTP server , and unpacks them
        :param pattern (string) only download data which obey this regular expression file pattern (e.g. 20[0-9][0-9] to download from 2000 and onward)
        """
        ftproot="ftp://my.cmems-du.eu/Core/INSITU_GLO_TS_REP_OBSERVATIONS_013_001_b/CORIOLIS-GLOBAL-EasyCORA-OBS/global"
        
        #get  cmems authentication details from database
        cred=self.conf.authCred("cmems")
        ftpcr=ftpCrawler(ftproot,auth=cred, pattern=pattern)
        
        updated=ftpcr.parallelDownload(self.cacheDir(),check=True,maxconn=10,continueonError=True)
        
        #unpack the downloaded files in the data directory
        datadir=self.dataDir()
        for tarf in [UriFile(f) for f in findFiles(self.cacheDir(),".*tgz$")]:
            succesfile=os.path.join(datadir,os.path.basename(tarf.url)+".isextracted")
            try:
                #check if the files need unpacking (only unpack when needed)
                    #check if the last file is already extracted
                    if os.path.exists(succesfile):
                        slurplogger().info(f"{tarf.url} is already extracted, skipping")
                    else:
                        with tarfile.open(tarf.url,"r:gz") as tf:
                            slurplogger().info(f"Extracting trajectory files from {tarf.url}")
                            tar_safe_extractall(tf,datadir)
                            #touch the sucessfile to indcate this archive has been sucessfully extracted
                        Path(succesfile).touch()
            except tarfile.ReadError as exc:
                raise exc

    def register(self,pattern='.*\.nc$'):
        """Register downloaded trajectory files from CORA
        :param pattern (string) file pattern to look for (defaults to all files ending with .nc)
        """
        #create a list of files which need to be (re)registered
        newfiles=self.retainnewUris([UriFile(file) for file in findFiles(self.dataDir(),pattern)])
        for uri in newfiles:
            meta=coraMetaExtractor(uri)
            if not meta:
                #don't register empty entries
                continue

            self.addEntry(meta)
        self._dbinvent.data["Description"]="EasyCora output data table"
        self._dbinvent.data["CORAversion"] = "5.2"
        self.updateInvent()

geoslurpCatalogue.addDataset(EasyCora)
