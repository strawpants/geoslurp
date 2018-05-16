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
import psycopg2 
from psycopg2 import sql
from psycopg2.extras import DictCursor
import datetime
import json
from .dbTablestructure import dataSourceEntry

class geoslurpClient():
    """Interface between the geoslurp data and database (currently postgresql)"""
    def __init__(self,dbscheme):
        #open up a database connector
        self._dbcon=psycopg2.connect(dbscheme)
        self.invent=dataSourceEntry()
    
    def getDSentry(self,name):
        """retrieves registered datasource entry from the inventory table in the database by its registered name"""
        with self._dbcon.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory WHERE datasource = %s",(name,))
            tmp=cur.fetchone()
            if tmp:
                return tmp
            else:
                tmp=self.invent.default
                tmp['datasource']=name
                return tmp

    def setDSentry(self,dataSource):
        """insert/update a datasource registry to the database"""
        ds=self.invent.scheme.validate(dataSource)
        dsupdate=self.invent.schemeupdate.validate(ds)
        with self._dbcon.cursor() as cur:
            #insert/update a new entry
            query=sql.SQL("INSERT INTO inventory ({}) values ({}) ON CONFLICT (datasource) DO UPDATE SET {}").format(
                sql.SQL(',').join(map(sql.Identifier,ds.keys())),
                sql.SQL(',').join(map(sql.Placeholder,ds.keys())),
                sql.SQL(',').join(map(lambda x:  sql.Identifier(x)+sql.SQL(" = excluded.")+sql.Identifier(x),dsupdate.keys())))
            cur.executemany(query,(ds,))
        self._dbcon.commit()

    def registerInventory(self):
        """ initialize the inventory table of the postgresql database"""
        self.invent.initDB(self._dbcon)

    def registerPlugin(self,Plugin):
        """Register a plugin (add entry in inventory and initialize a dataset table"""
        Plugin.initDB(self._dbcon)
