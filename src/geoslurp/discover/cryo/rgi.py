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

from sqlalchemy import select,union


def RGIQuery(dbcon):
    regions = ['01_rgi60_alaska', '02_rgi60_westerncanadaus', '03_rgi60_arcticcanadanorth',
               '04_rgi60_arcticcanadasouth', '05_rgi60_greenlandperiphery', '06_rgi60_iceland', '07_rgi60_svalbard',
               '08_rgi60_scandinavia', '09_rgi60_russianarctic','10_rgi60_northasia', '11_rgi60_centraleurope',
               '12_rgi60_caucasusmiddleeast', '13_rgi60_centralasia', '14_rgi60_southasiawest',
               '15_rgi60_southasiaeast', '16_rgi60_lowlatitudes', '17_rgi60_southernandes', '18_rgi60_newzealand',
               '19_rgi60_antarcticsubantarctic']
    tbl=dbcon.getTable(regions[0],'cryo')
    qry=select([tbl])
    for reg in regions[1:]:
        qry.union(dbcon.getTable(reg,'cryo'))

    return dbcon.dbeng.execute(qry)



def RGITQuery(dbcon,tablename):
    tbl=dbcon.getTable(tablename,'cryo')
    qry=select([tbl])
    return dbcon.dbeng.execute(qry)


