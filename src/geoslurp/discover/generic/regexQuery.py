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
from sqlalchemy import select,between

def regexQuery(dbcon,table,scheme="pubic",orderBy=None,tspan=None,**kwargs):
    """Retrieve entries from a table  by applying a regeular expressions query to specified columns"""

    #retrieve/reflect the table
    tbl = dbcon.getTable(table, scheme)
    qry = select([tbl])
    for col,regex in kwargs.items():
        qry=qry.where(getattr(tbl.c,col).op("~")(regex))
    
    if tspan:
        qry=qry.where(between(tbl.c.time,tspan[0],tspan[1]))

    if orderBy:
        qry=qry.order_by(getattr(tbl.c,orderBy))
    return dbcon.dbeng.execute(qry)
