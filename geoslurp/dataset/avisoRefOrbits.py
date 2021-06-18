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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from zipfile import ZipFile
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
from sqlalchemy import Column, Integer,ARRAY,String
from sqlalchemy.dialects.postgresql import TIMESTAMP
import re
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from math import modf

scheme='altim'

class AvisoRefOrbitBase(OGRBase):
    """Base class for Aviso reference orbit tracks"""
    scheme=scheme
    fromurl=""
    missionids=["j1a","j2a"]
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #update ogrfile
        self.ogrfile=os.path.join(self.cacheDir(),self.ogrfile)
    
    
    def valuesFromOgrFeat(self,feat,transform=None):
        """Extracts and adds the pass number from the feature"""
        vals=super().valuesFromOgrFeat(feat,transform)
        passmatch=re.search('(?:Ground Track|Pass) ([0-9]+)',vals['name'])
#         import pdb;pdb.set_trace()
        if passmatch:
            vals["pass"]=int(passmatch.group(1))
        vals["missionids"]=self.missionids
        return vals    
        


    def columnsFromOgrFeat(self,feat):
        cols=super().columnsFromOgrFeat(feat)
        cols.append(Column('pass', Integer))
        cols.append(Column('missionids', ARRAY(String)))
        return cols
    
    def pull(self):
        """Pulls the shapefile from the aviso server"""
        httpserv=http(self.fromurl,lastmod=datetime(2019,9,6))
        cache=self.cacheDir() 
        uri,upd=httpserv.download(cache,check=True)
        
#         if upd:
#             with ZipFile(os.path.join(cache,self.zipf),'r') as zp:
#                 zp.extractall(cache)


# Factory method to dynamically create classes
def AvisoClassFactory(kmzurl,missionids):
    splt=kmzurl.split("/")
#     splt=kmzurl.split("/")[-1].replace('-','_').replace('.','_').lower()[:-4]
    clsName=splt[-1].replace('-','_').replace('.','_').lower()[:-4]
    return type(clsName, (AvisoRefOrbitBase,), {"ogrfile":splt[-1],"fromurl":kmzurl,"swapxy":True,"gtype":"GEOMETRYZ","ignoreFields":"(timestamp)|(begin)|(end)|(altitude)|(tessellate)|(extrude)|(visibility)|(drawOrder)|(icon)|(snippet)","missionids":missionids})

def getAvisoRefOrbits(conf):
    """Automatically create all classes for the Aviso reference orbits"""
    out=[]
    rooturl='https://www.aviso.altimetry.fr/fileadmin/documents/missions/Swot/'
    swotversions=[('swot_science_orbit_sept2015-v2_10s.kmz','sw1t'),('swot_science_orbit_sept2015-v2_perDay.kml','sw1t'),('swot_science_hr_2.0s_4.0s_June2019-v3_perPass.kml','sw1a')]
    for kmzf,missionid in swotversions:
        out.append(AvisoClassFactory(rooturl+kmzf,[missionid]))

    rooturl2="https://www.aviso.altimetry.fr/fileadmin/documents/data/tools/"
    altversions=[('Visu_RefOrbit_J3J2J1TP_Tracks_GoogleEarth_V3.kmz',['txa','j1a','j2a','j3a']),('Visu_J1TP_Interlaced_Tracks_GE_V3.kmz',['txb','j1b','j2b']),
                ('Visu_J2_LRO_Cycle500_537.kmz',['j2c']),('Visu_EN_Tracks_GE_OldOrbit.kmz',['saa','e1','e2','en']),('Visu_ENN_Tracks_GE_NewOrbit.kmz',['enn']),('Visu_C2_Tracks_HiRes.kmz',['c2a']),
                ('Visu_G2_Tracks_GE_V3.kmz',['g2'])]
    for kmzf,misids in altversions:
        out.append(AvisoClassFactory(rooturl2+kmzf,misids))
     
    return out

geoslurpCatalogue.addDatasetFactory(getAvisoRefOrbits)


# #also setup tables to register cycles
@as_declarative(metadata=MetaData(schema=scheme))
class AltCycleTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id = Column(Integer, primary_key=True)
    cycle=Column(Integer)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    missionid=Column(String)

class AltCycleBase(DataSet):
    """Base class to register altimetry cycles"""
    url=None
    scheme=scheme
    auth=None
    fparse=None
    def __init__(self,dbconn):
            super().__init__(dbconn)
            AltCycleTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self):
        if self.auth:
            cred=self.conf.authCred(self.auth)
            fdown=http(self.url,auth=cred).download(self.cacheDir())
        else:
            fdown=http(self.url).download(self.cacheDir())

    def register(self):
        #currently deletes all entries in the table
        self.truncateTable()
        metalist=self.fparse(os.path.join(self.cacheDir(),os.path.basename(self.url)))
        for meta in metalist:
            self.addEntry(meta)
        self.updateInvent()

        

def parseCycleInfoJ1(file):
    """"Read and parse the Jason 1 cycle file and return a list of metadata dicts to be fed in the databasetable"""
    metalist=[]
    i=0
    with open(file,'rt') as fid:
        for ln in fid:
            if re.search('^[0-9]+\s+[12]',ln):
                splt=ln.split()
                decimalsec,sec=modf(float(splt[7]))
                tstart=datetime(year=int(splt[1]),month=int(splt[3]),day=int(splt[4]),hour=int(splt[5]),minute=int(splt[6]),second=int(sec),microsecond=int(decimalsec*1e6))
                if i > 0:
                    metalist[i-1]["tend"]=tstart
                cycle=int(splt[0])
                if cycle < 260:
                    missionid="j1a"
                elif 260 <= cycle <= 375:
                    missionid="j1b"
                else:
                    missionid='j1c'
                metalist.append({"cycle":int(splt[0]),"tstart":tstart,"missionid":missionid})
                i+=1
    metalist[-1]["tend"]=datetime(2013,6,21,hour=0,minute=56,second=54,microsecond=int(0.146e6))
    return metalist

def parseCycleInfoJ2(file):
    """"Read and parse the Jason 2 cycle file and return a list of metadata dicts to be fed in the databasetable"""
    metalist=[]
    i=0
    with open(file,'rt') as fid:
        for ln in fid:
            if re.search('^[0-9]+\s+[12]',ln):
                splt=ln.split()
                decimalsec,sec=modf(float(splt[7]))
                tstart=datetime(year=int(splt[1]),month=int(splt[3]),day=int(splt[4]),hour=int(splt[5]),minute=int(splt[6]),second=int(sec),microsecond=int(decimalsec*1e6))
                if i > 0:
                    metalist[i-1]["tend"]=tstart
                cycle=int(splt[0])
                if cycle < 305:
                    missionid="j2a"
                elif 305 <= cycle <= 327:
                    missionid="j2b"
                else:
                    missionid='j2c'
                metalist.append({"cycle":int(splt[0]),"tstart":tstart,"missionid":missionid})
                i+=1
    metalist[-1]["tend"]=datetime(2013,6,21,hour=0,minute=56,second=54,microsecond=int(0.146e6))
    return metalist
                    

def AltCycleClassFactory(url,fparse,auth=None):
    clsName=os.path.basename(url)[:-4]
    table=type(clsName +"Table", (AltCycleTBase,), {})
    return type(clsName, (AltCycleBase,), {"url": url, "table":table,"fparse":staticmethod(fparse),"auth":auth})



def getAltCycles(conf):
    out=[]
    url='https://podaac-tools.jpl.nasa.gov/drive/files/allData/jason1/L2/docs/j1_cyclelist.txt'
    out.append(AltCycleClassFactory(url,parseCycleInfoJ1))
    
    url='https://podaac-tools.jpl.nasa.gov/drive/files/allData/ostm/preview/L2/docs/ostm_cyclist.txt'
    
    out.append(AltCycleClassFactory(url,parseCycleInfoJ2,"podaac"))
    return out


geoslurpCatalogue.addDatasetFactory(getAltCycles)


