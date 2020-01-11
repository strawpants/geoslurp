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

def getFesomRunInfo(dbcon,runname):
    """return a dictionary with info on the registered run"""
    #first query the inventory on the requested run
    scheme='fesom'
    invent=dbcon.getInvent(runname,scheme)
    vtablename="vertices_"+invent.data["grid"]
    #extract info on the vertices table
    vertinvent=dbcon.getInvent(vtablename,scheme)
    rundict={"run":{"runTable":runname,"datadir":invent.datadir},"mesh":{"vertTable":vtablename,"zlevels":vertinvent.data["zlevels"]}}

    return rundict

def fesomMeshQuery(dbcon, fesominfo, geoWKT):
    """queries the geoslurp database for a valid vertices of a FESOM grid"""

    #retrieve/reflect the table
    tbl=dbcon.getTable(fesominfo["mesh"]["vertTable"],'fesom')

    qry=select([tbl.c.topo, tbl.c.nodeid,literal_column('geom::geometry').label('geom')])



    qry=qry.where(func.ST_within(literal_column('geom::geometry'),func.ST_GeomFromText(geoWKT,4326)))

    return dbcon.dbeng.execute(qry)

def fesomDataQuery(dbcon, fesominfo, tspan, interval=None):
    """Query a fesom run for datafiles"""

    #retrieve/reflect the table
    tbl=dbcon.getTable(fesominfo["run"]["runTable"],'fesom')

    qry=select([tbl]).where(or_(and_(tbl.c.tstart <= tspan[0],tspan[0] <= tbl.c.tend), and_(tbl.c.tstart <= tspan[1], tspan[1] <= tbl.c.tend)))

    if interval:
        qry=qry.where(tbl.c.interval == interval)

    return dbcon.dbeng.execute(qry)

