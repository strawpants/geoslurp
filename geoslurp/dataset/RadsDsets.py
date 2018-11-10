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
import os
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
from glob import glob
import logging
import re

geotracktype = Geography(geometry_type="LINESTRINGZ", srid='4326', spatial_index=True,dimension=3)

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
    direction=Column(String)
    uri=Column(String, unique=True,index=True)
    data=Column(JSONB)
    geom=Column(geotracktype)


def radsMetaDataExtractor(uri):
    """Extract a dictionary with rads antries for the database"""
    logging.info("extracting dta from %s"%(uri.url))
    ncrads=ncDset(uri.url)
    track=ogr.Geometry(ogr.wkbLineString)

    for lon,lat in zip(ncrads["lon"][:],ncrads["lat"][:]):
        track.AddPoint(float(lon),float(lat))
    #reference time for rads
    t0=datetime(1985,1,1)
    mtch=re.search("p([0-9]+)c([0-9]+).nc",uri.url)
    meta={"lastupdate":uri.lastmod,
          "tstart":t0+timedelta(seconds=float(ncrads['time'][0].data)),
          "tend":t0+timedelta(seconds=float(ncrads['time'][-1].data)),
          "cycle":int(mtch.group(2)),
          "apass":int(mtch.group(1)),
          "uri":uri.url,
          "data":{},
          "geom":WKBElement(track.ExportToWkb(),srid=4326,extended=True)
          }

    return meta


class RadsBase(DataSet):
    """Base class for a satellite in the rads database
    """
    __version__=(0,0)
    table=None
    sat=None
    phase=None
    def __init__(self,scheme):
        super().__init__(scheme)
        self.updated=None
        self.radsdir=self.scheme.conf.getDir("","RadsDir")

        #initialize postgreslq table
        RadsTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self, cycle=None):
        """Pulls the data from the rads server
        :param cycle: only pulls data from a specific cycle
        """
        cred=self.scheme.conf.authCred("rads")

        url="rads.tudelft.nl::rads/data"

        #pull configuration data (xml files)
        rsync(url+"/conf",auth=cred).parallelDownload(radsdir,True)

        srcurl=os.path.join(url,self.sat,self.phase)
        desturl=os.path.join(self.radsdir,self.sat)
        if cycle:
            srcurl=os.path.join(srcurl,"c%03d"%(cycle))
            desturl=os.path.join(desturl,self.phase)
        if not os.path.exists(desturl):
            os.makedirs(desturl)
        self.updated=rsync(srcurl,auth=cred).parallelDownload(desturl,True)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in glob(os.path.join(self.radsdir,self.sat,self.phase,'*/*.nc'))]


        ses=self.scheme.db.Session()


        for uri in files:
            try:
                base=os.path.basename(uri.url)
                qResult=ses.query(self.table).filter(self.table.uri.like('%'+base+'%')).first()
                if qResult.lastupdate >= uri.lastmod:
                    logging.info("No Update needed, skipping %s"%(base))
                    continue
                else:
                    #delete the entries which need updating
                    ses.delete(qResult)
                    ses.commit()
            except Exception as e:
                # Fine no entries found
                pass

            meta=radsMetaDataExtractor(uri)
            try:
                entry=self.table(**meta)
                ses.add(entry)

                if i > 10:
                    # commit every so many rows
                    ses.commit()
                    i=0
                else:
                    i+=1
            except Exception as e:
                pass
        self._inventData["lastupdate"]=datetime.now().isoformat()
        self._inventData["version"]=self.__version__
        self.updateInvent()

        ses.commit()




    def purge(self):
        pass

    def halt(self):
        pass

# Factory method to dynamically create classes
def radsclassFactory(clnm):
    dum,sat,phase=clnm.split("_")
    table=type(clnm+"Table",(RadsTBase,),{})
    return type(clnm, (RadsBase,), {"sat":sat,"phase":phase,"table":table})

def getRADSdict():
    """Automatically create all classes contained within the GSHHG database"""
    satphases={"j1":['a',"b"]}
    outdict={}
    for sat,phases in satphases.items():
        for  phase in phases:
            clname="rads_"+sat+"_"+phase
            outdict[clname]=radsclassFactory(clname)
    return outdict
