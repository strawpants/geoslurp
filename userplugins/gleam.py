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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022

from geoslurp.dataset.xarraybase import XarrayBase
from geoslurp.datapull.sftp import CrawlerSftp as crawler
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
import os
import tarfile
from scipy.io import loadmat
import xarray as xr
import numpy as np

class gleam_monthly(XarrayBase):
    outofdb=True
    scheme="prec_evap"
    groupby="time"
    writeoutofdb=False
    def pull(self):
        auth=self.conf.authCred("gleam",qryfields=["user","passw","url"])
        # note url should be of the form  sftp://server:port
        
        crwl=crawler(url=auth.url+"/data/v3.6b/monthly",auth=auth)
        downdir=self.dataDir()
        for uri in crwl.uris():
            uri.download(downdir)
    # def register(self):
        # self.xarfile=os.path.join(self.dataDir(),"Global_land_monthly_ET_V2.zarr")
        # tarar=os.path.join(self.cacheDir(),"Global_land_monthly_ET_V2.rar")
        # if not os.path.isdir(self.xarfile):
            # self.convert2zarr(tarar)
        
        # super().register()


geoslurpCatalogue.addDataset(gleam_monthly)
