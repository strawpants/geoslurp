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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2020

from geoslurp.view.viewBase import TView
from geoslurp.config.catalogue import geoslurpCatalogue


def subqry1(table,nmax,tol,apptypes=["GAA","GAB","GAC","GAD"]):
    tname="gravity.%s"%(table)
    sbqry="SELECT a.tstart+(a.tend-a.tstart)/2 AS time, a.uri AS gsm, b.uri AS gaa, c.uri AS gab, d.uri as gac, e.uri as gad from (SELECT tstart,tend,uri from %s WHERE type ='GSM' AND nmax = %d) a "\
            "INNER JOIN %s b ON ABS(EXTRACT(day FROM a.tstart-b.tstart)) < %d AND  ABS(EXTRACT(day FROM a.tend-b.tend)) < %d AND b.type = 'GAA' "\
            "INNER JOIN %s c ON ABS(EXTRACT(day FROM a.tstart-c.tstart)) < %d AND ABS(EXTRACT(day FROM a.tend-c.tend)) < %d AND c.type = 'GAB' "\
            "INNER JOIN %s d ON ABS(EXTRACT(day FROM a.tstart-d.tstart)) < %d AND ABS(EXTRACT(day FROM a.tend-d.tend)) < %d AND d.type = 'GAC' "\
            "INNER JOIN %s e ON ABS(EXTRACT(day FROM a.tstart-e.tstart)) < %d AND ABS(EXTRACT(day FROM a.tend-e.tend)) < %d AND e.type = 'GAD' "%(tname,nmax,tname,tol,tol,tname,tol,tol,tname,tol,tol,tname,tol,tol)
    return sbqry

def buildGSML2qry(gtable,gfotable,nmax,tol=8):
    qry="%s UNION %s ORDER BY time"%(subqry1(gtable,nmax,tol),subqry1(gfotable,nmax,tol))
    return qry

class GRACECOMB_L2_JPL_n96(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_jpl_rl06","gracefol2_jpl_rl06",96) 

class GRACECOMB_L2_JPL_n60(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_jpl_rl06","gracefol2_jpl_rl06",60) 

class GRACECOMB_L2_GFZ_n60(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_gfz_rl06","gracefol2_gfz_rl06",60) 

class GRACECOMB_L2_GFZ_n96(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_gfz_rl06","gracefol2_gfz_rl06",96) 

class GRACECOMB_L2_CSR_n60(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_csr_rl06","gracefol2_csr_rl06",60) 

class GRACECOMB_L2_CSR_n96(TView):
    scheme="gravity"
    qry=buildGSML2qry("gracel2_csr_rl06","gracefol2_csr_rl06",96) 

geoslurpCatalogue.addView(GRACECOMB_L2_JPL_n96)
geoslurpCatalogue.addView(GRACECOMB_L2_JPL_n60)

geoslurpCatalogue.addView(GRACECOMB_L2_GFZ_n96)
geoslurpCatalogue.addView(GRACECOMB_L2_GFZ_n60)

geoslurpCatalogue.addView(GRACECOMB_L2_CSR_n96)
geoslurpCatalogue.addView(GRACECOMB_L2_CSR_n60)



