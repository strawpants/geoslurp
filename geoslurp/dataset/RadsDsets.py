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
from geoalchemy2.elements import WKBElement
from geoalchemy2.types import Geography
from sqlalchemy import Column,Integer,String, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from geoslurp.datapull import UriFile
from geoslurp.datapull.rsync import Crawler as rsync
from geoslurp.datapull.uri import findFiles
import os
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
from glob import glob
from geoslurp.config.slurplogger import slurplogger
import re
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.db.settings import getCreateDir
geotracktype = Geography(geometry_type="MULTILINESTRINGZ", srid='4326', spatial_index=True, dimension=3)

@as_declarative(metadata=MetaData(schema='altim'))
class RadsTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    cycle=Column(Integer)
    apass=Column(Integer)
    uri=Column(String, unique=True,index=True)
    data=Column(JSONB)
    geom=Column(geotracktype)

def is_set(x,n):
    """Check if the nth bit of x is set to True"""
    return x & 1 << n != 0

def flag4_isonLand(x):
    return is_set(x,4)

def radsMetaDataExtractor(uri):
    """Extract a dictionary with rads entries for the database"""
    slurplogger().info("extracting data from %s"%(uri.url))
    ncrads=ncDset(uri.url)
    track=ogr.Geometry(ogr.wkbMultiLineString)
    data={"segments":[]}
   

    if ncrads.dimensions['time'].size <3:
       #no point trying to index empty files
       return {}
    #reference time 
    t0=datetime(1985,1,1)
    
    # We need to compare some values fromt he previous loop which we store in the follwoing variables 
    lonprev=ncrads["lon"][0]
    if lonprev > 180:
        lonprev-=360
    
    tprev=ncrads["time"][0]
    onlandprev=flag4_isonLand(ncrads["flags"][0])
   
   #initiate first linestring segment 
    trackseg=ogr.Geometry(ogr.wkbLineString)
    #we also store some bookkeeping information on each track segment
    segment={"tstart":(t0+timedelta(seconds=float(tprev))).isoformat(),"tend":None,"istart":0,"iend":0,"land":int(flag4_isonLand(ncrads["flags"][0]))}
   
    
    for i,(t,lon,lat,flag) in enumerate(zip(ncrads["time"][:],ncrads["lon"][:],ncrads["lat"][:],ncrads['flags'][:])):
        dt=t0+timedelta(seconds=float(t))
        onland=flag4_isonLand(flag)
        if lon > 180:
            #make sure longitude goes from -180 to 180
            lon-=360
            #create a new segment when: (a) crossing the 180 d line, (b) a gap larger than 100 seconds is occurred, (c) or when ocean/land flag changes
        if abs(lonprev-lon) > 180 or t-tprev > 100 or (onlandprev != onland):
            #start a new segment upon crossing the 180 line or when a time gap occurredi, or when crossing from land to ocean or lake
            #Segments which have more than a single point will be added:
            if trackseg.GetPointCount() > 1:
                #gather some end bookkeeping on the previous segment
                segment["tend"]=dt.isoformat()
                segment["iend"]=i
   
                #append segment and bookkeeping data
                data["segments"].append(segment.copy())
                # import pdb;pdb.set_trace()
                track.AddGeometry(trackseg)
            #initialize new segment
            segment["tstart"]=dt.isoformat()
            segment["istart"]=i
            segment["land"]=int(onland)
            trackseg=ogr.Geometry(ogr.wkbLineString)
        
        trackseg.AddPoint(float(lon),float(lat),0)
        lonprev=lon
        tprev=t
        onlandprev=onland
    
    #also add the last segment
    if trackseg.GetPointCount() > 1:
        #gather some end bookkeeping on the previous segment
        segment["tend"]=dt.isoformat()
        segment["iend"]=i

        #append segment and bookkeeping data
        data["segments"].append(segment)
        track.AddGeometry(trackseg)
   
    if not data["segments"]:
       #return an empty dict when no segments are found
       return {}

    #reference time for rads
    mtch=re.search("p([0-9]+)c([0-9]+).nc",uri.url)
    meta={"lastupdate":uri.lastmod,
          "tstart":t0+timedelta(seconds=float(ncrads['time'][0])),
          "tend":t0+timedelta(seconds=float(ncrads['time'][-1])),
          "cycle":int(mtch.group(2)),
          "apass":int(mtch.group(1)),
          "uri":uri.url,
          "data":data,
          "geom":WKBElement(track.ExportToIsoWkb(),srid=4326,extended=True)
          }

    return meta


class RadsBase(DataSet):
    """Base class for a satellite + phase in the rads database
    """
    table=None
    sat=None
    phase=None
    scheme='altim'
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.updated=None

        if not self._dbinvent.datadir:
            if 'RADSDATAROOT' in os.environ:
                self._dbinvent.datadir=getCreateDir(os.environ['RADSDATAROOT'])
            else:
                self._dbinvent.datadir=self.conf.getDir(self.scheme,"DataDir")
            self.updateInvent(False)
        #initialize postgreslq table
        self.table.__table__.create(self.db.dbeng,checkfirst=True)
        # RadsTBase.metadata.tables[".".join([self.scheme.lower(),self.name])].create(self.db.dbeng,checkfirst=True)
        # RadsTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self, cycle=None):
        """Pulls the data from the rads server
        :param cycle: only pulls data from a specific cycle
        """
        cred=self.conf.authCred("rads")

        url="rads.tudelft.nl::rads/data"

        #pull configuration data (xml files)
        rsync(url+"/conf",auth=cred).parallelDownload(self._dbinvent.datadir,True)

        srcurl=os.path.join(url,self.sat,self.phase)
        desturl=os.path.join(self._dbinvent.datadir,self.sat)
        if cycle:
            srcurl=os.path.join(srcurl,"c%03d"%(cycle))
            desturl=os.path.join(desturl,self.phase)
        getCreateDir(desturl)
        self.updated=rsync(srcurl,auth=cred).parallelDownload(desturl,True)

    def register(self,cycle=None,since=None):
        if since:
           since=datetime.strptime(since,"%Y-%m-%d")
           print(since)
        else:
           since=self._dbinvent.lastupdate
        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            slurplogger().info("Listing files to process (this can take a while)...")

            if cycle:
                files=[UriFile(file) for file in findFiles(os.path.join(self._dbinvent.datadir,self.sat,self.phase,"c%03d"%(cycle)),'.*\.nc$',since=since)]
            else:
                files=[UriFile(file) for file in findFiles(os.path.join(self._dbinvent.datadir,self.sat,self.phase),'.*\.nc$',since=since)]
        if not files:
           slurplogger().info("No updated files found")
           return

        newfiles=self.retainnewUris(files)
        if not newfiles:
            slurplogger().info("Nothing to update")
            return

        for uri in newfiles:
            meta=radsMetaDataExtractor(uri)
            if not meta:
               #don't register empty entries
               continue

            self.addEntry(meta)

        self.updateInvent()






# Factory method to dynamically create classes
def radsclassFactory(clnm):
    dum,sat,phase=clnm.split("_")
    table=type(clnm+"Table",(RadsTBase,),{})
    return type(clnm, (RadsBase,), {"sat":sat,"phase":phase,"table":table})

def getRADSDsets(conf):
    """Create all tables for all satellite missions and phases"""
    satphases={"j1":["a","b","c"],"j2":["a","b","c"],"j3":["a"],"3a":["a"],"c2":["a"],"n1":["b","c"],"sa":["a","b"],"tx":["a","b","n"]}
    out=[]
    for sat,phases in satphases.items():
        for  phase in phases:
            clname="rads_"+sat+"_"+phase
            out.append(radsclassFactory(clname))
    return out

geoslurpCatalogue.addDatasetFactory(getRADSDsets)
