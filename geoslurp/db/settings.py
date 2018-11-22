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

# reads and writes contains a class to work with  the geoslurp inventory table
from sqlalchemy import Column,Integer,String,Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.ext.mutable import MutableDict

GSBase=declarative_base()

class SettingsTable(GSBase):
    """Defines the GEOSLURP POSTGRESQL inventory table"""
    __tablename__='settings'
    id=Column(Integer, primary_key=True)
    user=Column(String, unique=True)
    conf=Column(MutableDict.as_mutable(JSONB))
    auth=Column(BYTEA) # stored as blowfish encrypted bytearray


class Settings():
    """Read and write default and user specific settings to and from the database"""
    table=SettingsTable
    def __init__(self,dbconn):
        self.db=dbconn
        #creates the settings table if it doesn't exists
        if not self.db.dbeng.has_table('settings'):
            GSBase.metadata.create_all(self.db.dbeng)
            #set default entry
            self.defaultentry=self.table(user='geoslurp',conf={"DataDir":"./geoslurp/data","CacheDir":"./geoslurp/cache"})

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self, key):
        return self._confDict[key]

    def __setitem__(self, key, val):
        self._confDict[key]=val

    def authCred(self,service):
        pass

    def addUser(self,name,passw):
        pass

    def update(self):
        """Update the postgresql settings table"""
        pass

