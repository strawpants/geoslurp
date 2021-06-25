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

#contains a class to work with  the geoslurp inventory table
from sqlalchemy import Column,Integer,String,Float,DateTime,ARRAY,JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy import MetaData
scheme="admin"
GSBase=declarative_base(metadata=MetaData(schema='admin'))

class InventTable(GSBase):
    """Defines the GEOSLURP POSTGRESQL inventory table"""
    __tablename__='inventory'
    id=Column(Integer, primary_key=True)
    scheme=Column(String)
    dataset=Column(String,unique=True)
    pgfunc=Column(String,unique=True)
    view=Column(String,unique=True)
    owner=Column(String)
    lastupdate=Column(DateTime)
    updatefreq=Column(Integer)
    version=Column(ARRAY(Integer,as_tuple=True))
    cache=Column(String)
    datadir=Column(String)
    data=Column(MutableDict.as_mutable(JSONB))
        
class Inventory:
    """Class which provides read/write access to the postgresql inventory table"""
    table=InventTable
    def __init__(self,geoslurpConn):
        """

        :type geoslurpConn: geoslurp database connector
        """
        self.db=geoslurpConn
        self._ses=self.db.Session()

        #creates the inventory table if it doesn't exists
        if not geoslurpConn.dbeng.has_table(self.table.__tablename__):
            GSBase.metadata.create_all(geoslurpConn.dbeng)
            #also grant geoslurp all privileges
            self.db.dbeng.execute('GRANT ALL PRIVILEGES ON admin.inventory to geoslurp;')
            self.db.dbeng.execute('GRANT USAGE ON SEQUENCE admin.inventory_id_seq to geoslurp')

            #read only user's may need to access information in the inventory table
            self.db.dbeng.execute('GRANT SELECT ON admin.inventory to geobrowse;')


    def __iter__(self):
        """Query the Inventory table and returns a generator"""
        for entry in self._ses.query(InventTable):
            yield entry

    def __getitem__(self, dataset):
        """Retrieves the entry from the inventory table corresponding to the dataset
        :param dataset: Table to be searched for. This can be either a table name (without the scheme) or as scheme.table"""
        #we need to open up a small sqlalcheny session here
        # note  this will raise a NoResultsFound exception if none was found (should be treated by caller)
        #when a dot is present we also need to check for the scheme
        spl=dataset.split(".")
        if len(spl) == 1:
            return self._ses.query(InventTable).filter(InventTable.dataset == spl[0]).one()
        else:
            return self._ses.query(InventTable).filter(InventTable.dataset == spl[1]).filter(InventTable.scheme == spl[0]).one()


