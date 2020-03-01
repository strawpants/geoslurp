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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020


import re
from sqlalchemy import select,and_,func,join
from geoslurp.config import slurplog
from datetime import timedelta


def joinByPeriod(left,right):
    """Convenience function to make an inner table join based upon similar start times"""
    dttol=timedelta(days=3)
    return join(left,right,and_(func.overlaps(left.c.tstart-dttol,left.c.tstart+dttol,right.c.tstart-dttol,right.c.tstart+dttol)))

def queryGRACE(dbcon,gravtablename,withAtm=False,withOceAtm=False,withOce=False,withSurfP=False):
    """Query GRACE solutions and add accompanying background products"""

    if re.search("itsg",gravtablename):
        #Special treatment using table joins because the backgroundmodels are in a different table

        pass
    else:
        # retrieve/reflect the table
        tbl = dbcon.getTable(gravtablename, 'gravity')
        #get the different types from the table
        qry = select([tbl.c.tstart,tbl.c.tend,tbl.c.uri.label("gsm")]).where(tbl.c.uri.like("%/GSM%BA%")).alias('subq1')

        if withAtm:
            gaa = select([tbl.c.tstart,tbl.c.tend,tbl.c.uri.label("gaa")]).where(tbl.c.uri.like("%/GAA%")).alias('qgaa')
            j=joinByPeriod(qry,gaa)
            qry=select([qry,gaa.c.gaa]).select_from(j).alias("subq2")
            # qry=select([qry,gaa.c.gaa]).where(func.overlaps(qry.c.tstart,qry.c.tend,gaa.c.tstart,gaa.c.tend)).alias("subq2")

        if withOceAtm:
            gac = select([tbl.c.tstart,tbl.c.tend,tbl.c.uri.label("gac")]).where(tbl.c.uri.like("%/GAC%")).alias('qgac')
            j=joinByPeriod(qry,gac)
            qry=select([qry,gac.c.gac]).select_from(j).alias("subq3")


        if withOce:
            gab = select([tbl.c.tstart,tbl.c.tend,tbl.c.uri.label("gab")]).where(tbl.c.uri.like("%/GAB%")).alias('qgab')
            j=joinByPeriod(qry,gab)
            qry=select([qry,gab.c.gab]).select_from(j).alias("subq4")


        if withSurfP:
            gad = select([tbl.c.tstart,tbl.c.tend,tbl.c.uri.label("gad")]).where(tbl.c.uri.like("%/GAD%")).alias('qgad')
            j=joinByPeriod(qry,gad)
            qry=select([qry,gad.c.gad]).select_from(j).alias("subq5")


        # slurplog.debug(str(qry))
        return dbcon.dbeng.execute(select([qry]).order_by(qry.c.tstart))




