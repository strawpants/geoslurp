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

from schema import Schema, And, Use
from psycopg2.extras import Json
import datetime 
class dataSourceEntry():
    """ Default data source entry for the inventory table """
    def __init__(self):
        self.scheme=Schema({"datasource":And(str,len),"lastupdate":And(lambda dt:dt.isoformat()),"version":Use(lambda tp: type(tp) is tuple),"datadir":And(str,len),"structure":Use(Json),"data":Use(Json)})
        #scheme which only retains the to be updated elements
        self.schemeupdate=Schema({"lastupdate":And(lambda dt:dt.isoformat()),"data":object}, ignore_extra_keys=True)
        self.default=self.scheme.validate({"datasource":"DUMMY", "lastupdate":datetime.datetime(datetime.MINYEAR,1,1),"version":(0,0,0),"datadir":" ","structure":{},"data":{}})
    def initTable(self,dbc):
        """ initializes the corresponding database table. NOTE: this function may require elevated priviliges"""
        with dbc.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS inventory ( id serial PRIMARY KEY, datasource varchar UNIQUE, lastupdate timestamp, version int[3],datadir varchar, structure jsonb, data jsonb )")
        dbc.commit()

class dataSetEntry():
    """Default dataset entry (one datasource may contain several datasets)"""
    def __init__(self,dsname,dsstruct):
        self.name=dsname
        self.dsstruct=dsstruct
        self.scheme=Schema({"lastupdate":And(lambda dt:dt.isoformat())})
        self.default=self.scheme.validate({"lastupdate":datetime.datetime(datetime.MINYEAR,1,1)},ignore_extra_keys=True)

    def initTable(self,dbc):
        """ initializes the corresponding database table. NOTE: this function may require elevated privileges"""
        with dbc.cursor() as cur:
            query=sql.SQL("CREATE TABLE IF NOT EXISTS {} ( id serial PRIMARY KEY, lastupdate timestamp, {} )").format(sql.Identifier(self.name),
                    SQL(',').join(map(lambda key,val:sql.Identifier(key)+' '+sql.Identifier(val),self.dsstruct)))
            cur.execute(query)
        
        dbc.commit()

