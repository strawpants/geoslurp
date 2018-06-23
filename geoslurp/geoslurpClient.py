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
# from schema import Schema, And, Use
# import psycopg2 
# from psycopg2 import sql
# from psycopg2.extras import DictCursor, Json
# import datetime
# import json
# from .dbTablestructure import dataSourceEntry
# from collections import OrderedDict


from sqlalchemy import create_engine,text,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
from sqlalchemy.orm import sessionmaker,mapper
from sqlalchemy.schema import CreateSchema,DropSchema,Table
from osgeo import ogr
from geoalchemy2.elements import WKBElement
from geoalchemy2 import Geometry
from .slurpconf import Log
import re

GSBase=declarative_base()
class Invent(GSBase):
    """Defines the POSTGRESQL inventory table"""
    __tablename__='inventory'
    id=Column(Integer,primary_key=True)
    datasource=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    pluginversion=Column(ARRAY(Integer,as_tuple=True))
    data=Column(JSONB)

def tableMapFactory(name,table):
    """Dynamically create a mapper class from a Table object"""
    return type(name,(GSBase,),{'__table__':table})

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
    
    def CreateSchema(self,name):
        try:
            self.dbeng.execute(CreateSchema(name.lower()))
        except:
            pass


    def dropSchema(self,name,cascade=False):
        self.dbeng.execute(DropSchema(name.lower(),cascade=cascade))

    def dropTable(self,tablename,schema):
        self.dbeng.execute('DROP TABLE IF EXISTS %s."%s;"'%(schema,tablename))

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
                    import ipdb
                    ipdb.set_trace()
                    pass
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
        print("Filling CSV table %s:%s with data from"%(schema,tablename),file=Log)
        names,cols=columnsFromCSV(fid,lookup)
        table=Table(tablename,self.mdata,*cols,schema=schema)
        table.create(checkfirst=True)
            
        tableMap=tableMapFactory(tablename,table)
        for ln in fid:
            values=valuesFromCSV(ln,names)
            ses.add(tableMap(**values)) 
        ses.commit()
        ses.close()
