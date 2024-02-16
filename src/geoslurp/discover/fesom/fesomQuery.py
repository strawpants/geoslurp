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


from sqlalchemy import select,func,asc,and_,or_,literal_column
from geoslurp.tools.shapelytools import shpextract
import numpy as np

def getFesomRunInfo(dbcon,runname):
    """return a dictionary with info on the registered run"""
    #first query the inventory on the requested run
    scheme='fesom'
    invent=dbcon.getInventEntry(runname,scheme)
    vtablename="vertices_"+invent.data["grid"]
    #extract info on the vertices table
    vertinvent=dbcon.getInventEntry(vtablename,scheme)
    rundict={"run":{"runTable":runname,"datadir":invent.datadir},"mesh":{"vertTable":vtablename,"datadir":vertinvent.datadir,"zlevels":vertinvent.data["zlevels"]}}

    return rundict
    

def fesomMeshQuery(dbcon, fesominfo, geoWKT=None):
    """queries the geoslurp database for a valid vertices of a FESOM grid"""
    #retrieve/reflect the table
    tbl=dbcon.getTable(fesominfo["mesh"]["vertTable"],'fesom')
    qry=select([tbl.c.topo, tbl.c.nodeid,literal_column('geom::geometry').label('geom')])
    
    if geoWKT:
        qry=qry.where(func.ST_within(literal_column('geom::geometry'),func.ST_GeomFromText(geoWKT,4326)))
       
    return dbcon.dbeng.execute(qry)
    # qryResult=dbcon.dbeng.execute(qry)

def fesomMeshQueryXY(dbcon, fesominfo, geoWKT):
    """Return fesom mesh grid as [lon, lat, depth]"""
    pnt=[]
    pnt_id=[]
    for entry in fesomMeshQuery(dbcon, fesominfo, geoWKT):
            tmp=shpextract(entry)
            xy = np.array([[tmp.x, tmp.y]])
            pnt_id.append(entry._row[1])
            tmp_id = np.array(entry._row[1])
            # tri_temp = 
            if len(pnt)==0:
                pnt=xy
                # pnt_id2= tmp_id
            else: 
                pnt = np.concatenate((pnt,xy)) 
                # pnt_id2 = np.concatenate((pnt_id2,tmp_id)) 
                # pnt_id2.extend(tmp_id)
            
    return pnt, pnt_id
    # return pnt,pnt_id,pnt_id2

def fesomDataQuery(dbcon, fesominfo, tspan, interval=None):
    """Query a fesom run for datafiles"""

    #retrieve/reflect the table
    tbl=dbcon.getTable(fesominfo["run"]["runTable"],'fesom')

    # qry=select([tbl]).where(or_(and_(tbl.c.tstart <= tspan[0],tspan[0] <= tbl.c.tend),
                                # and_(tbl.c.tstart <= tspan[1], tspan[1] <= tbl.c.tend)))

    qry=select([tbl]).where(func.overlaps(tbl.c.tstart,tbl.c.tend,tspan[0],tspan[1]))
    
    if interval:
        qry=qry.where(tbl.c.interval == interval)

    return dbcon.dbeng.execute(qry)

def fesomDataQueryURI(dbcon, fesominfo, tspan, interval=None):
    out = []
    for entry in fesomDataQuery(dbcon, fesominfo, tspan, interval=None):
        tmpdict = {"uri": entry._row[5], "tstart": entry.tstart, "tend": entry.tend}
        out.append(tmpdict)
    return out

    
def closest2Fesom(dbcon, fesominfo, geoWKT=None, samplePoints=[]):
    """returns vertices of a FESOM grid that have the smallest distance to sample points"""
    
    #retrieve/reflect the table
    tbl=dbcon.getTable(fesominfo["mesh"]["vertTable"],'fesom')
    qry=select([tbl.c.topo, tbl.c.nodeid,literal_column('geom::geometry').label('geom')])
    
    if geoWKT:
        qry=qry.where(func.ST_within(literal_column('geom::geometry'),
                                     func.ST_GeomFromText(geoWKT,4326)))   
    if len(samplePoints)!=0:
        pp=[]
        for p in samplePoints:
            # print(p)
            qry1=qry.order_by(func.ST_Distance(literal_column('geom::geometry'),
                                              func.ST_GeomFromText(p.wkt,4326)))
            qry1=qry1.limit(1)
            pp.append(dbcon.dbeng.execute(qry1).first()._row)
    
    return pp
    

def fesomMeshQueryWKB(dbcon, fesominfo, geoWKT):
    """Query the database positions of OBP points"""
    
    return [(shpextract(x)) for x in fesomMeshQueryXY(dbcon, fesominfo, geoWKT)]