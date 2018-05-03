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
from psycopg2.extras import DictCursor,Json
from schema import Schema, And, Use
import datetime
import json

class dataSourceSchema():
    def __init__(self):
        self.scheme=Schema({"datasource":And(str,len),"lastupdate":And(lambda dt:dt.isoformat()),"misc":Use(Json)})
        #scheme which only retains the to be updated elements
        self.schemeupdate=Schema({"lastupdate":And(lambda dt:dt.isoformat()),"misc":object}, ignore_extra_keys=True)
        self.default=self.scheme.validate({"datasource":"DUMMY", "lastupdate":datetime.datetime(datetime.MINYEAR,1,1),"misc":{}})
    def initTable(self,dbc):
        """ initializes the corresponding database table"""
        with dbc.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS inventory ( id serial PRIMARY KEY, datasource varchar UNIQUE, lastupdate timestamp, misc jsonb )")
        dbc.commit()
#dataDescriptorSchema=Schema({"dataSource":And(str, len)})


class geoslurpClient():
    """Interface between the geoslurp data and database (currently postgresql)"""
    def __init__(self,dbscheme):
        #open up a database connector
        self._dbcon=psycopg2.connect(dbscheme)
        self.invent=dataSourceSchema()
        cur=self._dbcon.cursor()
        self.invent.initTable(self._dbcon)

    def getDataSource(self,name):
        """retrieves registered data Source entry from the database"""
        with self._dbcon.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM inventory WHERE datasource = %s",(name,))
            tmp=cur.fetchone()
            if tmp:
                return tmp
            else:
                tmp=self.invent.default
                tmp['datasource']=name
                return tmp

    def setDataSource(self,dataSource):
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

    def putDataDescriptor(self,dataDescriptor):
        """Adds a datadescriptor entry to the database"""
        # dd=dataDescriptorSchema.validate(dataDescriptor)
        # self._db[dataDescriptor["dataSource"]].insert(dd) 
    
    def findDataDescriptor(self,dataSourceName,query):
        """Perform a query for datadescriptor on the database"""
        # self._db[dataSourceName].find(query) 

    #setup query constructors
    def withinTquery(self,tmin,tmax):
        """Creates a query restricting the epochs"""
    
    def withinGEOquery(self,poly):
        """Creates a query restricting the geographical region (e.g. within a polygon or bounding box)"""

    def withinSHquery(self,nmax,nmin):
        """Creates a query restricting the minimum and maximum spherical harmonic degree"""

    def setQueryAlias(self,name,query):
        """Register an alias for a query"""
        self._db.queryAliases.insert({"alias":name,"query":query})
    
    def getQueryAlias(self,alias):
        return self._db.queryAliases.find({"alias":name})
   
