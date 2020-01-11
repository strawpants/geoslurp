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

def radsQuery(dbcon, sattable, polyWKT,tspan=None,cycle=None):
    """queries the geoslurp database for segments of altimetry tracks within a specified geometry and/or timespan and or cycle"""
            
    #retrieve/reflect the table
    tbl=dbcon.getTable(sattable,'altim')

    qry=select([tbl.c.uri,literal_column('geom::geometry').label('geom')])
    # qry=select([tbl.c.uri,tbl.c.geom])

    if tspan:
        qry=qry.where(and_(tbl.c.tstart > tspan[0],tbl.c.tend < tspan[1]))
    
    if cycle:
        qry=qry.where(tbl.c.cycle == cycle)

    #add geospatial constraint
    # ogrpoly=lonlat2ogr(polygon)
    qry=qry.where(func.ST_intersects(tbl.c.geom,func.ST_GeomFromText(polyWKT,4326)))

    qry=qry.order_by(asc(tbl.c.tstart))


    # print(qry)
    return dbcon.dbeng.execute(qry)

