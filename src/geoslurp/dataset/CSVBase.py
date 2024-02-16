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

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplogger
from sqlalchemy import Table,Column, Integer, String, Float
from geoslurp.db import tableMapFactory
import re

def columnsFromCSV(line, lookup,sep=','):
    """reads column descriptors from comma separated values and creates a list of sqlalchemy columns"""
    names = []
    cols = [Column('id', Integer, primary_key=True)]
    ln = line.split(sep)
    for key in ln:
        ky = key.strip()
        names.append(ky.lower())
        cols.append(Column(ky.lower(), lookup[ky]))
    return names, cols


def valuesFromCSV(line,names,sep=','):
    vals={}
    for ky,x in zip(names,line.split(sep)):
        val=x.strip()
        if not bool(val):
            continue
        vals[ky] = val
    return vals

class CSVBase(DataSet):
    """Base class which downloads reads in a CSV table and registers it in a db table"""
    table=None
    csvfile=None
    separator=','
    lookup=None
    hskip=0
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
    def register(self):
        """Update/populate a database table  from a CSV file). This function reads all rows from an open CSV file. The first line is expected to hold the COlumn names, which are mapped to types in the lookup string dictionary
    """
        # currently we can only cope with updating the entire table as a whole
        self.db.dropTable(self.name,self.scheme)

        slurplogger().info("Filling CSV table %s.%s with data from %s" % (self.scheme, self.name, self.csvfile))
    
        with open(self.csvfile,'r') as fid:
            for i in range(self.hskip):
                next(fid)
            names,cols=columnsFromCSV(fid.readline(),self.lookup)
            table=Table(self.name,self.db.mdata, *cols, schema=self.scheme)
            table.create(checkfirst=True)
            self.table=tableMapFactory(self.name, table)

            for ln in fid:
                self.addEntry(valuesFromCSV(ln,names))


        #also update entry in the inventory table
        self.updateInvent()

