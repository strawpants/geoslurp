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
# from geoalchemy2.elements import WKBElement
from datetime import datetime,timedelta
from geoslurp.tools.time import dt2yearlyinterval,dt2monthlyinterval,decyear2dt
import os
from zipfile import ZipFile
from osgeo import ogr
from geoslurp.config.slurplogger import slurplogger
from geoslurp.config.catalogue import geoslurpCatalogue


scheme="oceanobs"

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

#define a declarative baseclass for spherical harmonics gravity  data
@as_declarative(metadata=MetaData(schema=scheme))
class PSMSLTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id = Column(Integer, primary_key=True)
    statname=Column(String)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    ndat=Column(Integer)
    countrycode=Column(String)
    formerid=Column(String)
    data=Column(JSONB)
    geom=Column(geoPointtype)

class PSMSLBase(DataSet):
    """Base class to store RLR/MET annual and monthly data"""
    url=None
    typ=None
    freq=None
    scheme=scheme
    def __init__(self,dbconn):
        super().__init__(dbconn)
        PSMSLTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self):
        http(self.url).download(self.cacheDir())
        zpf=os.path.join(self.cacheDir(),os.path.basename(self.url))
        #unzip file
        with ZipFile(zpf,'r') as zp:
            zp.extractall(self.cacheDir())

    def register(self):

        #currently deletes all entries in the table
        self.truncateTable()

        #open main index file and read
        zipdir=self.cacheDir()+"/"+self.typ+"_"+self.freq

        with open(os.path.join(zipdir,'filelist.txt'),'r') as fid:
            for ln in fid:
                lnspl=ln.split(";")
                lat=float(lnspl[1])
                lon=float(lnspl[2])
                id=int(lnspl[0])
                slurplogger().info("Indexing %s"%(lnspl[3]))

                geoLoc=ogr.Geometry(ogr.wkbPoint)
                geoLoc.AddPoint(lon,lat)
                meta={
                    "id":id,
                    "statname":lnspl[3],
                    "countrycode":lnspl[4],
                    "formerid":lnspl[5],
                    "geom":geoLoc.ExportToWkt(),
                    # "geom":WKBElement(geoLoc.ExportToWkb(),srid=4326,extended=True),
                }
                #also open data file
                data={"time":[],"sl":[]}
                tmin=datetime.max
                tmax=datetime.min
                with open(os.path.join(zipdir,'data',"%d.%sdata"%(id,self.typ))) as dfid:
                    for dln in dfid:
                        tyear,valmm,dum1,dum2=dln.split(";")
                        dt=decyear2dt(float(tyear))
                        if self.freq == 'monthly':
                            dstart,dend=dt2monthlyinterval(dt)
                        else:
                            #yearly
                            dstart,dend=dt2yearlyinterval(dt)
                        tmin=min(dt,tmin)
                        tmax=max(dt,tmax)

                        data["time"].append(dt.isoformat())
                        data["sl"].append(1e3*int(valmm))

                #open documentation files
                with open(os.path.join(zipdir,'docu',"%d.txt"%(id))) as docid:
                    data["doc"]=docid.readlines()
                #open auth file
                with open(os.path.join(zipdir,'docu',"%d_auth.txt"%(id))) as docid:
                    data["auth"]=docid.readlines()

                meta['tstart']=tmin
                meta["tend"]=tmax
                meta["data"]=data

                self.addEntry(meta)
            self.updateInvent()

def PSMSLClassFactory(clsName):
    dum,typ,freq=clsName.lower().split("_")
    url="http://www.psmsl.org/data/obtaining/"+typ+"."+freq+".data/"+typ+"_"+freq+".zip"
    table=type(clsName +"Table", (PSMSLTBase,), {})
    return type(clsName, (PSMSLBase,), {"url": url, "table":table,"typ":typ,"freq":freq})


def getPSMSLDsets(conf):
    out=[]
    for clsName in ["psmsl_rlr_monthly","psmsl_rlr_annual","psmsl_met_monthly"]:
        out.append(PSMSLClassFactory(clsName))
    return out


geoslurpCatalogue.addDatasetFactory(getPSMSLDsets)
