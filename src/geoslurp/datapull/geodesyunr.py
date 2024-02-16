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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from geoslurp.datapull import CrawlerBase,UriBase
from geoslurp.datapull.http import Uri as http
import os
from datetime import datetime

class Uri(UriBase):
    """derived class which additionally holds info from the inventory"""
    def __init__(self,indict):
        super().__init__(indict["uri"],lastmod=indict["lastupdate"])
        self.dict=indict

    def __getitem__(self, item):
        return self.dict[item]

class Crawler(CrawlerBase):
    """Crawl the gps tenv3 data on geodesy.unr.edu"""
    def __init__(self,catalogfile):
        super().__init__(url="http://geodesy.unr.edu")
        self.catalogfile=catalogfile

    def uris(self,refresh=True):
        """List uris of available gps final data in tenv3 format"""
        if refresh:
            #retrieve and parse the dataholdings
            http(os.path.join(self.rooturl,"NGLStationPages/DataHoldings.txt")).download('',outfile=self.catalogfile)

        with open(self.catalogfile,'rb') as fid:
            for cnt,ln in enumerate(fid):
                if cnt == 0:
                    continue
                lnspl=ln.decode("utf-8").split()
                lon=float(lnspl[2])
                if lon > 180:
                    lon=lon-360
                tstart=datetime.strptime(lnspl[7],"%Y-%m-%d")
                tend=datetime.strptime(lnspl[8],"%Y-%m-%d")
                lastupdate=datetime.strptime(lnspl[9],"%Y-%m-%d")
                statname=lnspl[0]
                frame="IGS08"
                url=os.path.join(self.rooturl,"gps_timeseries/tenv3/",frame,statname+"."+frame+".tenv3")

                meta={"statname":statname,
                      "lat":float(lnspl[1]),
                      "lon":lon,
                      "height":float(lnspl[3]),
                      "xyz":[float(lnspl[4]), float(lnspl[5]), float(lnspl[6])],
                      "uri":url,
                      "tstart":tstart,
                      "tend":tend,
                      "lastupdate":lastupdate,
                      "nsolu":int(lnspl[10])
                      }

                yield Uri(meta)

