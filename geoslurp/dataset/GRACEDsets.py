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
from geoslurp.datapull.webdav import Crawler as WbCrawler
from geoslurp.config import Log
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from glob import glob
#define a declarative baseclass for level2 GRACE data
@as_declarative(metadata=MetaData(schema='gravity'))
class GRACEL2TBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    nmax=Column(Integer)
    type=Column(String)
    uri=Column(String, unique=True,index=True)
    gm=Column(Float)
    re=Column(Float)
    tidesystem=Column(String)
    data=Column(JSONB)

def graceMetaExtractor(uri):
    """Extract meta information from a GRACE file"""
    pass

class GRACEL2Base(DataSet):
    """Derived type representing GRACE spherical harmonic coefficients on the podaac server"""
    release=None
    center=None
    table=None
    updated=None
    def __init__(self,scheme):
        super().__init__(scheme)
        #initialize postgreslq table
        GRACEL2TBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self):
        cred=self.scheme.conf.authCred("podaac")
        url="https://podaac-tools.jpl.nasa.gov/drive/files/allData/grace/L2/"+self.center+"/"+self.release
        webdav=WbCrawler(url,auth=cred,pattern='G.*gz')
        self.updated=webdav.parallelDownload(self.dataDir(),check=True)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=glob(self.dataDir()+'/G*gz')

        #loop over files
        for grcfile in files:
            meta=graceMetaExtractor(grcfile)




    def purge(self):
        pass

def GRACEL2ClassFactory(clsName):
    """Dynamically construct GRACE Level 2 dataset classes"""
    base,center,release=clsName.split("_")
    table=type(clsName+"Table",(GRACEL2TBase,),{})
    return type(clsName, (GRACEL2Base,), {"release": release, "center":center,"table":table})

# setup GRACE datasets
def GRACEdict():
    outdict={}
    for release in ["RL06"]:
        for center in ["CSR", "GFZ", "JPL"]:
            clsName="GRACEL2"+"_"+center+"_"+release
            outdict[clsName]=GRACEL2ClassFactory(clsName)
    return outdict
