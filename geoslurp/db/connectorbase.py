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
import inspect
import os

def raiseNotImpl():
    fname=inspect.stack()[1].function
    raise NotImplementedError(f"Sorry {fname} is not (yet) implemented for this database type")

class GeoslurpConnectorBase():
    """Holds the base class for a connector to a geoslurp database"""
    cache=None
    localdataroot=None
    def __init__(self,cache=None,dataroot=None):
        """
        Sets the variables which all connectors have in common"""

        if dataroot:
            self.localdataroot=dataroot
        else:
            self.localdataroot=os.path.join(os.path.expanduser('~'),'geoslurp_data')
        
        if cache:
            self.cache=cache
        else:
            #default when not specified
            self.cache="/tmp/geoslurp_cache"


    def transsession(self):
        raiseNotImpl()
    
    def vacuumAnalyze(self, tableName, schema):
        """vacuum and analyze a certain table"""
        raiseNotImpl()

    def CreateSchema(self, schema,private=False):
        raiseNotImpl()
    
    def schemaexists(self,name):
        raiseNotImpl()

    def dropSchema(self, schema, cascade=False):
        raiseNotImpl()

    def createTable(self, tablename,columns,scheme=None,temporary=False,truncate=False,bind=None):
        """Creates a (temporary) table from sqlalchemy columns and returns the corresponding tablemapper"""

        raiseNotImpl()

    def truncateTable(self,tablename,scheme=None):
        raiseNotImpl()

    def dropTable(self, tablename, schema=None):
        raiseNotImpl()

    def getTable(self,tname,scheme="public"):
        raiseNotImpl()

    def getFunc(self,fname,scheme="public"):
        """returns a database function based up string names"""
        raiseNotImpl()

    def createView(self, viewname, qry,schema=None):
        raiseNotImpl()
    
    def dropView(self, viewname, schema=None):
        raiseNotImpl()

    def addUser(self,name,passw,readonly=False):
        """Adds a user to the database (note executing this functions requires appropriate database rights"""
        raiseNotImpl()



