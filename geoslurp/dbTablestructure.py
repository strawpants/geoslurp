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

class TableScheme():
    def __init__(self,name,scheme):
        self.tablename=name
        #note scheme is supposed to be a list of tuples containing
        #("column name", "postgres datatype and attributes",Scheme validation function_
        #set up schema
        schemdict={}
        for sc in scheme:
                schemdict[sc[0]]=sc[2]
        self.scheme=Schema(schemdict)
        
        #setup table structure
        self.tblStruct=OrderedDict([(sc[0],sc[1]) for sc in scheme]))

class InventoryTableSchemedataSourceEntry():
    """ Default data source entry for the inventory table """
    def __init__(self):
        self.scheme=Schema({"datasource":And(str,len),"lastupdate":And(lambda dt:dt.isoformat()),"version":And(lambda tp: type(tp) is tuple),"datadir":And(str,len),"structure":Use(Json),"data":Use(Json)})
        #scheme which only retains the to be updated elements
        self.schemeupdate=Schema({"lastupdate":And(lambda dt:dt.isoformat()),"data":object}, ignore_extra_keys=True)
        self.default=self.scheme.validate({"datasource":"DUMMY", "lastupdate":datetime.datetime(datetime.MINYEAR,1,1),"version":(0,0,0),"datadir":" ","structure":{},"data":{}})

class dataSetEntry():
    """Default dataset entry (one datasource may contain several datasets)"""
    def __init__(self,dsname,dsstruct):
        self.name=dsname
        self.dsstruct=dsstruct
        self.scheme=Schema({"lastupdate":And(lambda dt:dt.isoformat())})
        self.default=self.scheme.validate({"lastupdate":datetime.datetime(datetime.MINYEAR,1,1)},ignore_extra_keys=True)

