# This file is part of Frommle
# Frommle is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# Frommle is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with Frommle; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@wobbly.earth), 2019
from sqlalchemy import select,text

def gshhs(dbcon,res='i',groundingLine=True):
    """Query the gssh shoreline database"""
    tablename='gshhs_'+res
    tbl=dbcon.getTable(tablename,'globalgis')

    if groundingLine:
        qry=select([tbl]).where(text("level < 5 OR level = 6"))
    else:
        qry=select([tbl]).where(text("level <= 5"))

    return dbcon.dbeng.execute(qry)
