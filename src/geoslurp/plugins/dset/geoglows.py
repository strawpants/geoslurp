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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2025


from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
import os
from geoslurp.dataset.OGRBase import OGRBase

schema="geoglowsv2"



class GeoglowsGlobalStreams(OGRBase):
    """Provides a class to host the Geoglows streams GIS polylines"""
    schema=schema
    url="http://geoglows-v2.s3-us-west-2.amazonaws.com/hydrography-global/global_streams_simplified.gpkg"
    version=(2,0,0)
    ogrfile=os.path.join("global_streams_simplified.gpkg")
    gtype='GEOMETRY'
    swapxy=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),os.path.basename(self.url))


    def pull(self):
        """Pulls the geopackage data from the Amazon bucket and store it in the cache directory"""
        uri=http(self.url)
        basename=os.path.basename(self.url)
        uri.download(direc=self.cacheDir(),outfile=basename)


def getGeoglowsDsets(conf):
    return [GeoglowsGlobalStreams]

