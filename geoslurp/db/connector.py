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

from sqlalchemy import create_engine,text,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
from sqlalchemy.orm import sessionmaker,mapper
from sqlalchemy.schema import CreateSchema,DropSchema,Table
from sqlalchemy.orm.exc import NoResultFound
from osgeo import ogr
from geoalchemy2.elements import WKBElement
from geoalchemy2 import Geometry
from .slurpconf import Log
from sqlalchemy.sql.expression import literal_column
import re
from datetime import datetime
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

GSBase=declarative_base()
class Invent(GSBase):
    """Defines the GEOSLURP POSTGRESQL inventory table"""
    __tablename__='inventory'
    id=Column(Integer,primary_key=True)
    datasource=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    pluginversion=Column(ARRAY(Integer,as_tuple=True))
    data=Column(JSONB)

def tableMapFactory(name,table=None):
    """Dynamically create/retrieve a mapper class from a Table object"""
    try:
        #possibly this class was already defined (we can only define it once)
        return globals()[name]
    except KeyError:
        if table != None:
            return type(name,(GSBase,),{'__table__':table})
        else:
            raise Exception('When creating a new SQLAlchemy tablemap, table is needed')

def columnsFromFeat(feat,spatindex=True,forceGType=None):
    """Returns a list of columns from a osgeo feature"""
    gisMap={'String':String,'Integer':Integer,'Real':Float,'Float':Float}
    df=feat.GetDefnRef()
    cols=[Column('id',Integer,primary_key=True)]
    for i in range(feat.GetFieldCount()):
        fld=df.GetFieldDefn(i)
        name=fld.GetName()
        if name.lower() == 'id':
            #skip columns with id (will be  renewed)
            continue
        cols.append(Column(name.lower(),gisMap[fld.GetTypeName()]))

    #append geometry column
    if forceGType:
        gType=forceGType
    else:
        gType=feat.geometry().GetGeometryName()
    geomtype=Geometry(gType,srid='4326',spatial_index=spatindex)
    cols.append(Column('geom',geomtype))
    return cols

def columnsFromDict(indict):
    """ Map the first level entries of a dictionary to POSTGRESQL types"""
    typeMap={float:Float,int:Integer,dict:JSONB,datetime:TIMESTAMP,str:String}
    cols=[Column('id',Integer,primary_key=True)]
    for ky,val in indict.items():
        cols.append(Column(ky.lower(),typeMap[type(val)]))
    return cols


def valuesFromFeat(feat):
    """Returns a dictionary with loaded values from a feature"""
    df=feat.GetDefnRef()
    vals={}
    for i in range(feat.GetFieldCount()):
        fld=df.GetFieldDefn(i)
        name=fld.GetName()
        if name.lower() == 'id':
            #skip columns with id (will be automatically filled)
            continue
        if fld.GetTypeName() =='String':
            vals[name.lower()]=feat.GetFieldAsBinary(i).decode('iso-8859-1')
        else:
            vals[name.lower()]=feat.GetField(i)

    #append geometry values 
    vals['geom']=WKBElement(feat.geometry().ExportToWkb(),srid=4326)
    return vals

def columnsFromCSV(fid,lookup,skip=0):
    """reads the column descriptors from comma separated values and creates a list of sqlachemy columns"""
    csvMap={'String':String,'Integer':Integer,'Float':Float}
    for i in range(skip):
        next(fid)
    names=[] 
    cols=[Column('id',Integer,primary_key=True)]
    ln=fid.readline().split(',')
    for key in ln:
        ky=key.strip()
        names.append(ky.lower())
        cols.append(Column(ky.lower(),csvMap[lookup[ky]]))
    return names,cols

def valuesFromCSV(line,names):
    vals={}
    for ky,x in zip(names,line.split(',')):
        val=x.strip()
        if not bool(val):
            continue
        vals[ky]=val
    return vals

class geoslurpClient():
    """Holds some SQLalchemy database stuff"""
    
    def __init__(self,dburl):
       self.dbeng=create_engine(dburl,echo=False)
       self.Session=sessionmaker(bind=self.dbeng)
       self.mdata=MetaData(bind=self.dbeng)
       

       #creates the inventory table if it doesn't exists
       if not self.dbeng.has_table('inventory'):
           GSBase.metadata.create_all(self.dbeng)
    
    def vacuumAnalyze(self,name,schema):
        """vacuum and analyze a certain table"""
        conn=self.dbeng.raw_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor=conn.cursor()
        cursor.execute('VACUUM ANALYZE %s."%s";'%(schema,name))

    def CreateSchema(self,name):
        try:
            self.dbeng.execute(CreateSchema(name.lower()))
        except:
            pass

    def getFromInventory(self,datasource):
        """Retrieves the datasource entry from the inventory table"""
        #we need to open up a small sqlalcheny session here
        self.invses=self.Session()
        try:
            invententry=self.invses.query(Invent).filter(Invent.datasource == datasource).one()
        except NoResultFound:
            #in case of an exception we want to clsoet he session first (probably works without but it doesn't harm to be explicit )
            self.invses.close()
            raise NoResultFound

        return invententry

    def updateInventory(self,invententry):
        """updates an entry in the inventory"""
        self.invses.add(invententry)
        self.invses.commit()
        return invententry


    def dropSchema(self,name,cascade=False):
        self.dbeng.execute(DropSchema(name.lower(),cascade=cascade))

    def dropTable(self,tablename,schema):
        self.dbeng.execute('DROP TABLE IF EXISTS %s."%s";'%(schema,tablename))
    
    def updateFunction(self,fname,schema,inpara,outtype,body,language):
        """Updates a stored function"""
        #explicitly add line endings after semicolons in the body when not done already
        body=re.sub(';(?!\n)',';\n ',body)
        self.dbeng.execute('DROP FUNCTION IF EXISTS %s."%s";'%(schema,fname))
        funccmd='CREATE OR REPLACE FUNCTION %s.%s (%s) RETURNS %s AS $$ %s $$ LANGUAGE %s '%(schema,fname,inpara,outtype,body,language)
        self.dbeng.execute(funccmd)


    def fillGeoTable(self,folder,tablename,schema,regex=None,forceGType=None):
        """Update/populate a database table (creates one if it doesn't exist)
        This function reads all layers in the shapefile directory whose name obeys
        the regex and puts them in a single table.
        """
        table=None 
        ses=self.Session()
        # currently we can only cope with updating the entire table as a whole
        self.dropTable(tablename,schema)
        # if self.dbeng.has_table(tablename,schema=schema):
        print("Filling POSTGIS table %s:%s with data from"%(schema,tablename),folder,file=Log)
        #open shapefile directory
        shpf=ogr.Open(folder)
        for il in range(shpf.GetLayerCount()):
            #check for regex
            if regex:
                if not bool(re.search(regex,shpf[il].GetName())):
                    continue
            for ift in range(shpf[il].GetFeatureCount()):
                #we need to make a emporary clone here as osgeo will cause a segfault otherwise
                feat=shpf[il][ift].Clone()
                #print(feat.geometry().GetGeometryName(),file=Log)
                if table == None:
                    cols=columnsFromFeat(feat,forceGType=forceGType)
                    table=Table(tablename,self.mdata,*cols,schema=schema)
                    table.create(checkfirst=True)
                    tableMap=tableMapFactory(tablename,table)
                values=valuesFromFeat(feat)
                try:
                    ses.add(tableMap(**values))
                except:
                    pass
        ses.commit()
        # self.vacuumAnalyze(tablename,schema)
        ses.commit()
        ses.close()
    
    def fillCSVTable(self,fid,tablename,lookup,schema):
        """Update/populate a database table  from a CSV file)
        This function reads all rows from an open CSV file. The first line is expected to hold the COlumn names, which are mapped to types in the lookup string dictionary
        """
        table=None 
        ses=self.Session()
        # currently we can only cope with updating the entire table as a whole
        self.dropTable(tablename,schema)
        # if self.dbeng.has_table(tablename,schema=schema):
        print("Filling CSV table %s:%s "%(schema,tablename),file=Log)
        names,cols=columnsFromCSV(fid,lookup)
        table=Table(tablename,self.mdata,*cols,schema=schema)
        table.create(checkfirst=True)
            
        tableMap=tableMapFactory(tablename,table)
        for ln in fid:
            values=valuesFromCSV(ln,names)
            ses.add(tableMap(**values)) 
        ses.commit()
        # self.vacuumAnalyze(tablename,schema)
        ses.commit()
        ses.close()
