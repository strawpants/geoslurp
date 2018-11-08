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

# This file comtains some functions which aid in the construction of sqlalchemy columns

from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY, JSONB
from datetime import datetime



def columnsFromDict(indict):
    """ Map the first level entries of a dictionary to POSTGRESQL types"""
    typeMap = {float: Float, int: Integer, dict: JSONB, datetime: TIMESTAMP, str: String}
    cols = [Column('id', Integer, primary_key=True)]
    for ky, val in indict.xmlitems():
        cols.append(Column(ky.lower(), typeMap[type(val)]))
    return cols
