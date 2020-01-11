# This file is part of geoslurp-tools.
# geoslurp-tools is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# geoslurp-tools is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019
from sqlalchemy import select,func,asc,and_,literal_column

def duacsQuery(dbcon, name):
    """queries the geoslurp database for a gridded duacs altimetry dataset (by name)"""
            
    #retrieve/reflect the table
    tbl=dbcon.getTable('duacs','altim')

    qry=select([tbl.c.name,tbl.c.uri])
    qry=qry.where(tbl.c.name == name)


    return dbcon.dbeng.execute(qry)

