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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019

from geoslurp.dbfunc.dbfunc import DBFunc
from geoslurp.config.slurplogger import slurplogger

class gs_rads_subsegment(DBFunc):
    """Find subsegments in a rads table with the corresponding indices of the datafiles """
    scheme='altim'
    inargs="segments geom, inpoly geom, data jsonb" # jsonb dictionary, corresponding multipolygon, geometry to compare with"
    pgbody="BEGIN\nSELECT 1,ST_DumpPoints(segm) from (SELECT ST_dump(geom) as segm)END;\n" 
    language='sql'
    outargs="TABLE(id int, geom geography(LINESTRING))"
    def __init__(self,dbcon):
        super().__init__(dbcon)


    def register(self):
        """Register/update the function in the database"""
        self.createreplace()
    
