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


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
from sqlalchemy.orm import sessionmaker

GSBase=declarative_base()
class Invent(GSBase):
    """Defines the POSTGRESQL inventory table"""
    __tablename__='inventory'
    id=Column(Integer,primary_key=True)
    datasource=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    version=Column(ARRAY(Integer,as_tuple=True))
    data=Column(JSONB)


class geoslurpClient():
    """Holds some SQLalchemy database stuff"""
    
    def __init__(self,dburl):
       self.db=create_engine(dburl,echo=True)
       self.Session=sessionmaker(bind=self.db)
       GSBase.metadata.create_all(self.db)

# class TableScheme():
    # """ Simple class which holds the structure and type of a certain database table"""
    # def __init__(self,name,scheme):
        # self.tablename=name
        # #note scheme is supposed to be a list of tuples containing
        # #("column name", "postgres datatype and attributes",Scheme validation function_
        # #set up schema
        # schemdict={}
        # for sc in scheme:
                # schemdict[sc[0]]=sc[2]
        # self.scheme=Schema(schemdict)
        
        # #setup table structure
        # self.tblStruct=OrderedDict([(sc[0],sc[1]) for sc in scheme]))
    # def create(self,db):
        # """Creates a new table in the database if it does not exists"""
        # with db.cursor() as cur:
            # query=sql.SQL("CREATE TABLE IF NOT EXISTS {} ( {} )").format(sql.Identifier(self.tablename), 
                    # sql.SQL(',').join(map(lambda kv:sql.Identifier(kv[0])+sql.SQL(' ')+sql.Identifier(kv[1]),self.tblStruct.items())))  
            # cur.execute(query)


# class geoslurpClientold():
    # """Interface between the geoslurp data and database (currently postgresql)"""
    # def __init__(self,dbscheme):
        # #open up a database connector
        # self._dbcon=psycopg2.connect(dbscheme)

        # #possibly create inventory table not this needs to be an ordereddictionary in order to preserve the sequence
        # self.inventTable=TableScheme("inventory",[("id","serial PRIMARY KEY",And(int)),("datasource","varchar UNIQUE",And(str,len)),("lastupdate","timestamp",And(lambda dt:dt.isoformat()) ),("version","int[3]", And(lambda tp: type(tp) is tuple)),("data","jsonb",Use(Json))])
        # self.inventTable.create(self._dbconn)
    
    # def getInventoryEntry(self,dataSourceName):
        # """retrieves registered datasource entry from the inventory table in the database by its registered name"""
        # with self._dbcon.cursor(cursor_factory=DictCursor) as cur:
            # cur.execute("SELECT * FROM inventory WHERE datasource = %s",(dataSourceName,))
            # return cur.fetchone()

    # def updateInventoryEntry(self,dataSource):
        # """insert/update a datasource registry to the database"""
        # ds=self.inventTable.scheme.validate(dataSource)
        # dsupdate=self.invent.schemeupdate.validate(ds)
        # with self._dbcon.cursor() as cur:
            # #insert/update a new entry
            # query=sql.SQL("INSERT INTO inventory ({}) values ({}) ON CONFLICT (datasource) DO UPDATE SET {}").format(
                # sql.SQL(',').join(map(sql.Identifier,ds.keys())),
                # sql.SQL(',').join(map(sql.Placeholder,ds.keys())),
                # sql.SQL(',').join(map(lambda x:  sql.Identifier(x)+sql.SQL(" = excluded.")+sql.Identifier(x),dsupdate.keys())))
            # cur.executemany(query,(ds,))
        # self._dbcon.commit()
    

    # def registerPlugin(self,Plugin):
        # """Register a plugin (add entry in inventory and initialize a dataset table"""
        # Plugin.initDB(self._dbcon)
