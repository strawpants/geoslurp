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

from geoslurp.db import GSBase


def tableMapFactory(tableName, table=None):
    """Dynamically create/retrieve a mapper class from a Table object"""
    try:
        # possibly this class was already defined (we can only define it once)
        return globals()[tableName]
    except KeyError:
        if table != None:
            return type(tableName, (GSBase,), {'__table__': table})
        else:
            raise Exception('When creating a new SQLAlchemy tablemap, a table is needed')

