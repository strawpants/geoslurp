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
       self.dbeng=create_engine(dburl,echo=True)
       self.Session=sessionmaker(bind=self.dbeng)
       GSBase.metadata.create_all(self.dbeng)

