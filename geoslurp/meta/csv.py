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

#some functions to work with files with Comma Separated Values (CSV)

from sqlalchemy import Table,Column, Integer
from geoslurp.db import tableMapFactory
from geoslurp.config.slurplogger import slurplogger

def columnsFromCSV(line, lookup):
    """reads column descriptors from comma separated values and creates a list of sqlalchemy columns"""
    names = []
    cols = [Column('id', Integer, primary_key=True)]
    ln = line.split(',')
    for key in ln:
        ky = key.strip()
        names.append(ky.lower())
        cols.append(Column(ky.lower(), lookup[ky]))
    return names, cols


def valuesFromCSV(line,names):
    vals={}
    for ky,x in zip(names,line.split(',')):
        val=x.strip()
        if not bool(val):
            continue
        vals[ky] = val
    return vals

def fillCSVTable(filename,tablename,lookup,scheme,hskip=0):
    """Update/populate a database table  from a CSV file)
    This function reads all rows from an open CSV file. The first line is expected to hold the COlumn names, which are mapped to types in the lookup string dictionary
    """


    ses=scheme.db.Session()
    # currently we can only cope with updating the entire table as a whole
    scheme.dropTable(tablename)

    # if self.dbeng.has_table(tablename,schema=schema):
    slurplogger().info("Filling CSV table %s:%s "%(scheme._schema,tablename))
    with open(filename,'r') as fid:
        for i in range(hskip):
            next(fid)
        names,cols=columnsFromCSV(fid.readline(),lookup)
        table=Table(tablename,scheme.db.mdata, *cols, schema=scheme._schema)
        table.create(checkfirst=True)
        tableMap=tableMapFactory(tablename, table)

        for ln in fid:
            values=valuesFromCSV(ln,names)
            ses.add(tableMap(**values))

    ses.commit()
    # self.vacuumAnalyze(tablename,schema)
    # ses.commit()
    ses.close()
