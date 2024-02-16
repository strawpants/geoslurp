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
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020
from sqlalchemy import select

def queryRegex(dbcon,table,scheme="pubic",**kwargs):
    """Retrieve entries from a table applying gravity field table by querying with simple constraints"""

    print(scheme)
    for ky,val in kwargs.items():
        print(ky,val)

# retrieve/reflect the table
    # tbl = dbcon.getTable(table, 'gravity')
    # qry = select([tbl])
    # if typeregex:
        # qry=qry.where(tbl.c.type["name"].astext.op("~")(typeregex))
    # return dbcon.dbeng.execute(qry)
