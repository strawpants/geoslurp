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
from sqlalchemy import text
# from geoslurptools.aux.ogrgeom import lonlat2ogr
from sqlalchemy import select,func,asc,and_,literal_column,between
from geoalchemy2.functions import ST_Dump
from geoslurp.tools.shapelytools import shpextract

def coraQuery(dbcon,geoWKT=None,tspan=None,withinDmeter=None,tsort=None):
    tbl=dbcon.getTable('easycora','oceanobs')


    #first create a subquery to quickly discard argo profiles


    subqry=select([tbl])
    
    if tspan:
        subqry=subqry.where(func.overlaps(tbl.c.tstart,tbl.c.tend,tspan[0],tspan[1]))
   
    
    # Apply initial geospatial constraints 
    if geoWKT:
        if withinDmeter:
            #only base initial constraints ont he bounding box
            subqry=subqry.where(func.ST_DWithin(literal_column('ST_Envelope(geom::geometry)::geography'),func.ST_GeogFromText(geoWKT),withinDmeter))
        else:
            subqry=subqry.where(func.ST_Intersects(literal_column('geom::geometry'),func.ST_GeomFromText(geoWKT,4326)))
    
    
    #we need to assign an alias to this subquery in order to work with it
    subqry=subqry.alias("ar")
    #expand the arrays and points int he subquery
    qry=select([
        subqry.c.wmoid,
        subqry.c.uri,
        subqry.c.datacenter,
        func.unnest(subqry.c.mode).label('mode'),
        func.unnest(subqry.c.ascend).label('ascend'),
        func.unnest(subqry.c.tlocation).label('tlocation'),
        func.unnest(subqry.c.cycle).label('cycle'),
        func.unnest(subqry.c.iprof).label('iprof'),
        ST_Dump(literal_column("ar.geom::geometry")).geom.label('geom')])

    #additional spatial constraints
    finalqry=qry 
    qry=qry.alias("arex")
    
    if tspan:
        finalqry=select([qry]).where(between(qry.c.tlocation,tspan[0],tspan[1]))

    if geoWKT:
        if withinDmeter:
            #only base initial constraints ont he bounding box
            finalqry=finalqry.where(func.ST_DWithin(qry.c.geom,func.ST_GeogFromText(geoWKT),withinDmeter))
        else:
            finalqry=finalqry.where(func.ST_Within(literal_column("arex.geom"),func.ST_GeomFromText(geoWKT,4326)))

    if tsort:
        finalqry=finalqry.order_by(qry.c.tlocation)

    return dbcon.dbeng.execute(finalqry)

def queryMonthlyCora(dbcon, geoWKT, tstart, tend):
    """Query the database for lists of monthly Argo profiles within a certain polygon and time span"""
    
    out = {}

    for entry in coraQuery(dbcon,geoWKT=geoWKT,tspan=[tstart,tend],tsort=True):
        epoch=(entry.tlocation.year,entry.tlocation.month)
        pnt=shpextract(entry)
        tmpdict = {"uri": entry.uri, "iprof": entry.iprof, "lonlat": (pnt.x, pnt.y), "filetype": entry.datacenter}
        if not epoch in out:
            out[epoch] = [tmpdict]
        else:
            out[epoch].append(tmpdict)

    return out
