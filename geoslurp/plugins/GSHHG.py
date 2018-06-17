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

from geoslurp.dataProviders.ftpProvider import ftpProvider as ftp
from geoslurp.commonOptions import commonOptions
import os,re,sys
import datetime
import zipfile
from geoslurp.geoslurpClient import GSBase,Invent
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
from sqlalchemy.orm.exc import NoResultFound
from geoalchemy2.elements import WKBElement
from geoalchemy2 import Geometry,shape
from osgeo import ogr
from glob import glob
from shapely.geometry import shape
from sqlalchemy.ext.declarative import declarative_base,declared_attr


#define the abstract base table to be used a template for all tables
class GSHHSBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
    __table_args__ = {'schema': 'GSHHG'}
    id =  Column(Integer, primary_key=True)
    level=Column(Integer)
    source=Column(String)
    area=Column(Float)
    geom=Column(Geometry('POLYGON',srid='4326',spatial_index=True))

class WDBIIBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__
    __table_args__ = {'schema': 'GSHHG'}
    id =  Column(Integer, primary_key=True)
    level=Column(Integer)
    entity=Column(String)
    geom=Column(Geometry('LINESTRING',srid='4326',spatial_index=True))

GSHHSBase=declarative_base(cls=GSHHSBase)
WDBIIBase=declarative_base(cls=WDBIIBase)

class GSHHS_c(GSHHSBase):
    """Defines the crude dataset table for the GSHHG"""
class GSHHS_f(GSHHSBase):
    """Defines the full dataset table for the GSHHG"""
class GSHHS_h(GSHHSBase):
    """Defines the high resolution dataset table for the GSHHG"""
class GSHHS_i(GSHHSBase):
    """Defines the intermediate dataset table for the GSHHG"""
class GSHHS_l(GSHHSBase):
    """Defines the low resolution dataset table for the GSHHG"""

class WDBII_c(WDBIIBase):
    """Defines the crude dataset table for the GSHHG"""
class WDBII_f(WDBIIBase):
    """Defines the full dataset table for the GSHHG"""
class WDBII_h(WDBIIBase):
    """Defines the high resolution dataset table for the GSHHG"""
class WDBII_i(WDBIIBase):
    """Defines the intermediate dataset table for the GSHHG"""
class WDBII_l(WDBIIBase):
    """Defines the low resolution dataset table for the GSHHG"""



class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    
    #plugin version (needs to be updated for breaking changes)
    pluginVersion=(0,0,0)
    ###### COMPULSARY FUNCTIONS #######
    def __init__(self,db,conf):
        """Setup main urls, and retrieve already registered plugins from the database"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.name=type(self).__name__
        self.datadir=conf.getDataDir(self.name)
        self.cachedir=conf.getCacheDir(self.name)
        #Initialize databases (if not existent)
        self.ses=db.Session()
        self.dbeng=db.dbeng
        self.log=conf.log
        try:
            #retrieve the stored inventory entry
            self.dbinvent=self.ses.query(Invent).filter(Invent.datasource == self.name).one()
            self.dbinvent.data["GSHHGversion"]=tuple(self.dbinvent.data["GSHHGversion"]) 
        except NoResultFound:
        # #set defaults for the  inventory
            self.dbinvent=Invent(datasource=self.name,pluginversion=self.pluginVersion,lastupdate=datetime.datetime.min,data={"GSHHGversion":(0,0,0)})
            
            # create the schema and tables
            db.createSchema("GSHHG")
            GSHHSBase.metadata.create_all(db.dbeng)
            WDBIIBase.metadata.create_all(db.dbeng)
            self.ses.add(self.dbinvent)
        
    
    def parseAndExec(self,args):
        """Download/update data and apply possible processing"""
        if args.remove:
            print(self.name+":Removing SCHEMA and tables",file=self.log)
            self.remove()
        if args.update:
            self.download(args.force)

    @staticmethod
    def addParserArgs(subparsers):
        """adds GSHHG specific help options (note this is a static function)"""
        parser = subparsers.add_parser(GSHHG.__name__, help=GSHHG.__doc__)
        commonOptions['force'](parser)
        commonOptions['update'](parser)
        commonOptions['remove'](parser)

    ###### END COMPULSARY FUNCTIONS #######
    
    def download(self,force):
        ftplist=self.ftpt.getftplist('gshhg-shp.*zip')
        #first find out the newest version
        vregex=re.compile('gshhg-shp-([0-9]\.[0-9]\.[0-9]).*zip')
        newestver=(0,0,0)
        getf=''
        
        #find out the newest version
        for t,fname in ftplist:
            match=vregex.findall(fname)
            ver=tuple(int(x) for x in match[0].split('.'))
            if ver > newestver:
                newestver=ver
                getf=fname

        #now determine whether to retrieve the file
        if force or (newestver > self.dbinvent.data["GSHHGversion"] and t > self.dbinvent.lastupdate):
            fout=os.path.join(self.cachedir,getf)
            if os.path.exists(fout) and not force:
                print (self.name+":File already in cache no need to download",file=self.log)
            else:
                with open(fout,'wb') as fid:
                    print(self.name+":Downloading "+getf,file=self.log)
                    self.ftpt.downloadFile(getf,fid)
            self.unzip(fout)
            self.dbinvent.data["GSHHGversion"]=newestver
            self.dbinvent.lastupdate=datetime.datetime.now()
            self.updateTable(os.path.join(self.cachedir,'GSHHS_shp/c'),GSHHS_c)
            self.updateTable(os.path.join(self.cachedir,'GSHHS_shp/f'),GSHHS_f)
            self.updateTable(os.path.join(self.cachedir,'GSHHS_shp/h'),GSHHS_h)
            self.updateTable(os.path.join(self.cachedir,'GSHHS_shp/i'),GSHHS_i)
            self.updateTable(os.path.join(self.cachedir,'GSHHS_shp/l'),GSHHS_l)
            
            self.updateTable(os.path.join(self.cachedir,'WDBII_shp/c'),WDBII_c)
            self.updateTable(os.path.join(self.cachedir,'WDBII_shp/f'),WDBII_f)
            self.updateTable(os.path.join(self.cachedir,'WDBII_shp/h'),WDBII_h)
            self.updateTable(os.path.join(self.cachedir,'WDBII_shp/i'),WDBII_i)
            self.updateTable(os.path.join(self.cachedir,'WDBII_shp/l'),WDBII_l)
            #update database inventory
            self.ses.flush()
        else:
            print(self.name+": Already at newest version",file=self.log)
            return
        
    def unzip(self,zipf):
        print(self.name+": Unzipping shapefiles in cache directory")
        with zipfile.ZipFile(zipf,'r') as zp:
            zp.extractall(self.cachedir)
    
    def updateTable(self,folder,table):
        """update/populate a database table"""
        #lookup fr the field indices
        fieldDef={'id':0,'level':1,'source':2,'parent_id':3,'sibling_id':4,'area':5}
        wdbii=bool(re.search('WDBII',folder))
        #delete all records in the table (easier than update)
        ndel=self.ses.query(table).delete()

        print(self.name+":Filling POSTGIS with data from",folder,file=self.log)
        #open shapefile directory
        shpf=ogr.Open(folder)
        for il in range(shpf.GetLayerCount()):
            for ift in range(shpf[il].GetFeatureCount()):
                #we need to make a emporary clone here as osgeo will cause a segfault otherwise
                feat=shpf[il][ift].Clone()
                #create a database entry
                geom=WKBElement(feat.geometry().ExportToWkb(),srid=4326)
                if wdbii:
                    #retrieve the entity type from the layer name
                    entry=table(level=feat.GetFieldAsInteger(fieldDef['level']),entity=shpf[il].GetName().split('_')[1], geom=geom)
                
                else:
                    entry=table(level=feat.GetFieldAsInteger(fieldDef['level']),source=feat.GetFieldAsString(fieldDef['source']),area=feat.GetFieldAsDouble(fieldDef['area']), geom=geom)
                
                self.ses.add(entry)
            self.ses.commit()

    def remove(self):
        """Removes the database entries"""
        #remove entire GSHHG schema and the tables
        self.dbeng.execute('DROP SCHEMA "GSHHG" CASCADE;')

        #remove entry in the inventory
        self.ses.query(Invent).filter(Invent.datasource == self.name).delete()
        self.ses.commit() 

#define the plugin name
PlugName=GSHHG

