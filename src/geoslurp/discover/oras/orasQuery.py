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

# Author Alisa Yakhontova (yakhontova@geod.uni-bonn.de), Roelof Rietbroek (r.rietbroek@utwente.nl), 2020

from sqlalchemy import select,func,asc,and_,or_,literal_column
from geoslurp.tools.shapelytools import shpextract
import numpy as np
from netCDF4 import Dataset
from netCDF4 import num2date


def getOrasRunInfo(dbcon,runname):
    """return a dictionary with info on the registered run"""
    #first query the inventory on the requested run
    scheme='oras'
    invent=dbcon.getInventEntry(runname,scheme)
    vtablename="vertices_oras5_"+invent.data["grid"]
    #extract info on the vertices table
    vertinvent=dbcon.getInventEntry(vtablename,scheme)
    rundict={"run":{"runTable":runname,"datadir":invent.datadir},"mesh":{"vertTable":vtablename,"datadir":vertinvent.datadir}}

    return rundict


def orasDataQuery(dbcon, info, tspan):
    """Query a fesom run for datafiles"""

    #retrieve/reflect the table
    tbl=dbcon.getTable(info["run"]["runTable"],'oras')

    qry=select([tbl]).where(and_(tspan[0] <= tbl.c.tstart,tbl.c.tstart <= tspan[1]))
    
    return dbcon.dbeng.execute(qry)

def orasDataQueryURI(dbcon, info, tspan):
    out = []
    for entry in orasDataQuery(dbcon, info, tspan):
        a=entry
        tmpdict = {"uri": entry._row[3], "tstart": entry.tstart}
        out.append(tmpdict)
    return out



