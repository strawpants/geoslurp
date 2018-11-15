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
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from geoslurp.datapull.http import Uri as http
from geoalchemy2.types import Geography
from geoalchemy2.elements import WKBElement
from datetime import datetime
import os
from zipfile import ZipFile
from osgeo import ogr

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

#define a declarative baseclass for spherical harmonics gravity  data
@as_declarative(metadata=MetaData(schema='oceanobs'))
class PSMSLTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    nmax=Column(Integer)
    omax=Column(Integer)
    gm=Column(Float)
    re=Column(Float)
    tidesystem=Column(String)
    format=Column(String)
    type=Column(String)
    uri=Column(String, unique=True,index=True)
    data=Column(JSONB)

class PSMSLBase(DataSet):
    """Base class to store RLR/MET annual and monthly data"""
    url=None
    def pull(self):
        http(self.url).download(self.cacheDir())
        zpf=os.path.join(self.cacheDir(),os.path.basename(self.url))
        #unzip file
        with ZipFile(zpf,'r') as zp:
            zp.extractall(self.cacheDir())

    def register(self):
        #open main index file and read
        zipdir=self.cacheDir()+"/"+self.__class__.__name__[6:]

        with open(os.path.join(zipdir,'filelist.txt'),'r') as fid:
            for ln in fid:
                lnspl=ln.split(";")
                lat=float(lnspl[1])
                lon=float(lnspl[2])

                geoLoc=ogr.Geometry(ogr.wkbPoint)
                geoLoc.AddPoint(lon,lat)
                meta={
                    "id":int(lnspl[0]),
                    "name":lnspl[3],
                    "countrycode":lnspl[4],
                    "formerID":lnspl[5],
                    "geom":WKBElement(geoLoc.ExportToWkb(),srid=4326,extended=True),"data":{}})
                }


def PSMSLClassFactory(clsName):
    dum,typ,freq=clsName.lower().split("_")
    url="http://www.psmsl.org/data/obtaining/"+typ+"."+freq+".data/"+typ+"_"+freq+".zip"
    table=type(clsName +"Table", (PSMSLTBase,), {})
    return type(clsName, (PSMSLBase,), {"url": url, "table":table})


def getPSMSLDicts():
    outdict={}
    for clsName in ["psmsl_rlr_monthly","psmsl_rlr_annual","psmsl_met_monthly"]:
        outdict[clsName]=PSMSLClassFactory(clsName)
    return outdict

