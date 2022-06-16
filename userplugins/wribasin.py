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

from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.config.catalogue import geoslurpCatalogue
import urllib.request
from zipfile import ZipFile
import os

class WriBasin(OGRBase):
    """Base class for Wribasin watersheds """
    scheme='globalgis'
    swapxy=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),"wribasin.shp")

    def pull(self):
        """Pulls the wribasin data from the internet and unpacks it in the cache directory"""
        #wrisource=http("http://www.fao.org/geonetwork/srv/en/resources.get")
        wrisource=http("http://www.fao.org/geonetwork/srv/en/resources.get?id=30914&fname=wri_basins.zip&access=private")
        
        # urif, upd=wrisource.download(self.cacheDir(),check=True,outfile=os.path.join(self.cacheDir(),"wri_basin.zip"),postdic={"id":"30914","fname":"wri_basins.zip","access":"private"})
        urif, upd=wrisource.download(self.cacheDir(),check=True,outfile=os.path.join(self.cacheDir(),"wri_basin.zip"))

        if upd:
            with ZipFile(urif.url,'r') as zp:
                    zp.extractall(self.cacheDir())

geoslurpCatalogue.addDataset(WriBasin)
