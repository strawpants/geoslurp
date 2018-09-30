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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from geoslurp.dataset import DataSet
from geoslurp.datapull import OpendapConnector,OpendapFilter

class Argo(DataSet):
    """Argo table"""
    def __init__(self,scheme):
        super().__init__(scheme)


    def pull(self):
        """Get a list of netcdf files from the Ifremer opendap Thredds server"""

        conn=OpendapConnector([OpendapFilter(attr="urlPath",regex=".*profiles.*")])

        baseurl,xmlroot = conn.getCatalog("http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS/catalog.xml")
        print(conn.getopendapRoot(xmlroot))
        for ds in conn.dataSets(xmlroot, baseurl, depth=10):
            print(ds.tag, ds.attrib["urlPath"])

    def register(self):
        pass
