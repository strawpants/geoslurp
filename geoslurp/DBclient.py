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
from pymongo import MongoClient
class DBclient():
    """Interface between the geoslurp data and database (currently mongodb)"""
    def __init__(self,mongohost):
        self.conn=MongoClient(host=mongohost)

    def getPluginInfo(self,name):
        """retrieves registered plugin data from the database"""
        db=self.conn.PluginReg
        return db.find({"PluginName":name})

    def setPluginInfo(self,doc):
        """Sets a """
