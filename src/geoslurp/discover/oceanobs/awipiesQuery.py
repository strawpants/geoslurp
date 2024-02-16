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

# Author alisa yakhontova (yakhontova@geod.uni-bonn.de), 2020
from sqlalchemy import text
# from geoslurptools.aux.ogrgeom import lonlat2ogr
from sqlalchemy import select,func,asc,and_,literal_column,between
from geoalchemy2.functions import ST_Dump
from geoslurp.tools.shapelytools import shpextract
import shapely.geometry as geometry
from shapely.geometry import Point,Polygon,LineString

def awipiesQuery(dbcon,tspan=None, geoWKT=False):
    tbl=dbcon.getTable('awipies','oceanobs')
    
    #select time
    subqry=select([tbl])
    
    if tspan:
        subqry=subqry.where(func.overlaps(tbl.c.tstart,tbl.c.tend,tspan[0],tspan[1]))
   
    subqry=subqry.alias("ar")
    #expand the arrays and points int he subquery
    qry=select([
        subqry.c.id,
        subqry.c.name,
        subqry.c.uri,
        subqry.c.depth,
        literal_column('geom::geometry').label('geom')])
        # ST_Dump(literal_column("ar.geom::geometry")).geom.label('geom')])

    #additional spatial constraints
    finalqry=qry 
    qry=qry.alias("arex")
    
    return dbcon.dbeng.execute(finalqry)

def awipiesWKB(dbcon, tspan, geoWKT=False):
    """Query the database positions of OBP points"""
    
    return [(shpextract(x)) for x in awipiesQuery(dbcon,tspan)]

 


