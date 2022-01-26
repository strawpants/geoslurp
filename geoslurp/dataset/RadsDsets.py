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
from sqlalchemy.ext.declarative import declared_attr, as_declarative,declarative_base
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
from glob import glob
from geoslurp.config.slurplogger import slurplogger
import re
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.db.settings import getCreateDir
geotracktype = Geography(geometry_type="MULTILINESTRINGZ", srid='4326', spatial_index=True, dimension=3,from_text="ST_GeogfromWKB")

scheme='altim'


@as_declarative(metadata=MetaData(schema=scheme))
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
            #create a new segment when: (a) crossing the 180 d line, (b) or when ocean/land flag changes
        if abs(lonprev-lon) > 180 or (onlandprev != onland):
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
          "geom":track.ExportToIsoWkb()
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
                self._dbinvent.datadir=self.conf.getDataDir(self.scheme,subdirs="RADS")
            self.updateInvent(False)
        #initialize postgreslq table
        self.table.__table__.create(self.db.dbeng,checkfirst=True)
        # RadsTBase.metadata.tables[".".join([self.scheme.lower(),self.name])].create(self.db.dbeng,checkfirst=True)
        # RadsTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self, cycle=None,passes=None):
        """Pulls the data from the rads server
        :param cycle: only pulls data from a specific cycle
        """
        cred=self.conf.authCred("rads")

        url="rads.tudelft.nl::rads/data"


        #note we need the / at the end so we set the upstream root after the phase
        srcurl=os.path.join(url,self.sat,self.phase)+"/"

        desturl=os.path.join(self._dbinvent.datadir,self.sat,self.phase)
        getCreateDir(desturl)
        include=None
        if cycle or passes:
           #set up include constraints for rsync
            if cycle:
               #which directories to include in rsync filter
               if isinstance(cycle,list):
                  include=[f"/c{cyc:03d}" for cyc in cycle]
               else:
                  include=[f"/c{cycle:03d}"]
            else:
               include=[]

            if passes:
               if isinstance(passes,list):
                  include.extend([f"{self.sat}p{pss:04d}*nc" for pss in passes])
               else:
                  include.append(f"{self.sat}p{passes:04d}*nc")
            else:
               include.append(f"{self.sat}*nc")

        slurplogger().info(f"rsyncing rads data to {desturl}")
        self.updated=rsync(srcurl,auth=cred).parallelDownload(desturl,True,include)
      
    def register(self,cycle=None,since=None):
        if since:
           since=datetime.strptime(since,"%Y-%m-%d")
        else:
           since=self._dbinvent.lastupdate
        #create a list of files which need to be (re)registered
        if self.updated:
            files=[f for f in self.updated if f.url.endswith("nc")]
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



def extractCycleInfo(filename):
   cycledicts=[]
   #a 009 0149 0770 180606020805.000 180627212652.000  619 1262097
   missionid=os.path.basename(filename)[0:-4]
   frmt='%y%m%d%H%M%S.%f'
   with open(filename,'rt') as fid:
      for ln in fid.readlines():
         (ph,cycle,startpass,endpass,tstart,tend,npass,nobs)=ln.split()
         cycle=int(cycle)
         startpass=int(startpass)
         endpass=int(endpass)
         npass=int(npass)
         nobs=int(nobs)
         tstart=datetime.strptime(tstart,frmt)
         tend=datetime.strptime(tend,frmt)
         cycledicts.append(dict(cycle=cycle,missionid=missionid,startpass=startpass,endpass=endpass,tstart=tstart,tend=tend,npass=npass,nobs=nobs))

   return cycledicts

RadsCTBase=declarative_base(metadata=MetaData(schema=scheme))
class RadsCatalogueT(RadsCTBase):
    __tablename__="radscycles"
    id = Column(Integer, primary_key=True)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    cycle=Column(Integer)
    startpass=Column(Integer)
    endpass=Column(Integer)
    npass=Column(Integer)
    nobs=Column(Integer)
    missionid=Column(String,index=True)



class RadsCycles(DataSet):
   table=RadsCatalogueT
   scheme=scheme
   def __init__(self,dbconn):
      super().__init__(dbconn)
      self.updated=None
      #possibly use an external location for the data
      if not self._dbinvent.datadir:
         if 'RADSDATAROOT' in os.environ:
             self._dbinvent.datadir=getCreateDir(os.environ['RADSDATAROOT'])
         else:
             self._dbinvent.datadir=self.conf.getDataDir(self.scheme,subdirs="RADS")
         self.updateInvent(False)

   def pull(self):
      """Pulls the catalogues from the rads server 
      """
      cred=self.conf.authCred("rads")

      srcurl="rads.tudelft.nl::rads/tables"
      #pull table data files
      rsync(srcurl,auth=cred).parallelDownload(self.dataDir(),True)
      #also pull xml configurations

      srcurl="rads.tudelft.nl::rads/conf"
      #pull xml configuration files
      rsync(srcurl,auth=cred).parallelDownload(self.dataDir(),True)

   def register(self):
      #truncate table
      self.truncateTable()

      for cyclefile in glob(self.dataDir()+'/*.cyc'):
         slurplogger().info("extracting cycle catalogue from %s"%(cyclefile))
         cycleinfo=extractCycleInfo(cyclefile)
         self.bulkInsert(cycleinfo)
      self.updateInvent()


# Factory method to dynamically create classes
def radsclassFactory(clnm):
    dum,sat,phase=clnm.split("_")
    table=type(clnm+"Table",(RadsTBase,),{})
    return type(clnm, (RadsBase,), {"sat":sat,"phase":phase,"table":table})

def getRADSDsets(conf):
    """Create all tables for all satellite missions and phases"""


    satnph=["n1c","pna","g1a","j2b","3b3","c2a","e1b","gsb","e2a","e1e","3b2","j2c","6aa","e1f","j3a","j2a","j1b","j1c","gsa","3b5","3bb","3b0","txn","j1a","e1a","3b4","3aa","j2d","e1g","gsd","6a1","saa","txb","3b1","sab","3ba","n1b","e1c","e1d","txa"]

    # tphases={"j1":["a","b","c"],"j2":["a","b","c"],"j3":["a"],"3a":["a"],"c2":["a"],"n1":["b","c"],"sa":["a","b"],"tx":["a","b","n"],"3a":["a"]}
    out=[]
    for sat in satnph:
       clname="rads_"+sat[0:2]+"_"+sat[2:3]
       out.append(radsclassFactory(clname))
    return out

geoslurpCatalogue.addDatasetFactory(getRADSDsets)
geoslurpCatalogue.addDataset(RadsCycles)




#### RADS REFERENCE ORBITS (DEPENDS ON ABOVE dataset classes) ####
RadsRefOrbitTBase=declarative_base(metadata=MetaData(schema=scheme))
class RadsRefT(RadsRefOrbitTBase):
    __tablename__="radsreforbits"
    id = Column(Integer, primary_key=True)
    lastupdate=Column(TIMESTAMP)
    missionid=Column(String,index=True)
    refcycle=Column(Integer)
    apass=Column(Integer)
    geom=Column(geotracktype)


class RadsRefOrbits(DataSet):
   table=RadsRefT
   scheme=scheme
   def __init__(self,dbconn):
      super().__init__(dbconn)
      self.updated=None

   def pull(self, missionRegex=None):
      """Pulls the ncessary tables and data from the rads server 
      :param missionRegex: only register specific mission obeying this regular expression
      """
      
      #pulls and registers the radsCycle table if it needs updating
      radsCycles=geoslurpCatalogue.getDatasets(self.conf,f"{scheme}.RadsCycles")[0](self.db)
      if radsCycles.isExpired():
         radsCycles.pull()
         readsCycles.register()

      #determine the reference cycles and download rads 1hz data files to get the orbit
      self.registerRefCycles()

      #the query will take the first cycle which has the maximum amount of passes for each mission/-phase combination

      # Download appropriate cycles
      for mission,entry in self._dbinvent.data["missions"].items():
         if missionRegex:
            if not re.search(missionRegex,mission):
               slurplogger().info(f"Skipping mission {mission}")
               continue
         #Download  rads data for this specific cycle
         sat=mission[0:2]
         ph=mission[2:3]
         alttbl=f"{scheme}.rads_{sat}_{ph}"
         slurplogger().info(f"Getting reference cycle {entry['refcycle']} for {alttbl}")
         radsOrbit=geoslurpCatalogue.getDatasets(self.conf,f"{alttbl}")[0](self.db)
         radsOrbit.pull(cycle=entry["refcycle"])
         radsOrbit.register()



   def register(self,missionRegex=None):
      #Extract the orbit and parameters from the reference cycles, and put them in the appropriate table
      self.truncateTable() 
      if not "missions" in self._dbinvent.data:
         self.registerRefCycles()

      for mission,entry in self._dbinvent.data["missions"].items():
         if missionRegex:
            if not re.search(missionRegex,mission):
               slurplogger().info(f"Skipping mission {mission} ")
               continue
         sat=mission[0:2]
         ph=mission[2:3]
         alttbl=f"{scheme}.rads_{sat}_{ph}"
         if not self.db.tableExists(alttbl):
               slurplogger().info(f"Skipping mission {mission}, because {alttbl} does not exists")
               continue
         else:
            slurplogger().info(f"Registering mission {mission}")


         refqry=f"""
            INSERT INTO {self.scheme}.radsreforbits(lastupdate,missionid,refcycle,apass,geom)
            SELECT lastupdate, '{mission}' as missionid, cycle as refcycle, apass,
            ST_SimplifyPreserveTopology(geom::geometry,0.005) as geom
            FROM {alttbl} WHERE cycle = {entry["refcycle"]};
            """
         self.db.dbeng.execute(refqry)
      
      self.updateInvent()

   def registerRefCycles(self):
       
      cycleqry="""
         SELECT DISTINCT ON (missionid) missionid, npass, cycle AS cycle
         FROM altim.radscycles ORDER BY missionid,nobs DESC,npass DESC
         """
      #(store the reference cycles in the metadata of the table)
      self._dbinvent.data["missions"]={entry.missionid:{"refcycle":entry.cycle,"npass":entry.npass}  for entry in self.db.dbeng.execute(cycleqry)}
      self.updateInvent(updateTime=False)


geoslurpCatalogue.addDataset(RadsRefOrbits)
