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
from geoslurp.config.slurplogger import slurplog
from geoslurp.tools.cf import cfadd_coord
from datetime import datetime
import os
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
        downdir=self.cacheDir()
        for uri in crwl.uris():
            uri.download(downdir,check=True)

    def convert2zarr(self):
        slurplog.info("Converting data to zarr%s"%(self.xarfile))
        #open all datasets together
        ds=xr.open_mfdataset(os.path.join(self.cacheDir(),"*.nc"))
        cfadd_coord(ds.lon,'X',standard_name='longitude')
        cfadd_coord(ds.lat,'Y',standard_name='latitude')

        #save to zarr format
        ds.to_zarr(self.xarfile)

    def register(self):
        self.xarfile=os.path.join(self.dataDir(),"2003-2021_GLEAM_v3.6b_MO.zarr")
        if not os.path.isdir(self.xarfile):
            self.convert2zarr()
        
        super().register()


geoslurpCatalogue.addDataset(gleam_monthly)
