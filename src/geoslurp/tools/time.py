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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018
from datetime import datetime,timedelta

def decyear2dt(decyear):
    """Convert a decimal year to a datetime object"""
    year=int(decyear)
    jan1=datetime(year,1,1)
    return jan1+(decyear-year)*(datetime(year+1,1,1)-jan1)

def dt2monthlyinterval (dtin):
    """retrieves the start end end of a month"""
    if dtin.month != 12:
        endofmonth=datetime(dtin.year,dtin.month+1,1)-timedelta(seconds=1)
    else:
        endofmonth=datetime(dtin.year+1,1,1)-timedelta(seconds=1)
    return datetime(dtin.year,dtin.month,1),endofmonth

def dt2yearlyinterval(dtin):
    """Retrieves the start eand end of the year"""
    return datetime(dtin.year,1,1),datetime(dtin.year+1,1,1)-timedelta(seconds=1)

