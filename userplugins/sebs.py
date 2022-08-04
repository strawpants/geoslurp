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
from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
from geoslurp.config.slurplogger import slurplog
from geoslurp.tools.cf import *
import os
import tarfile
from scipy.io import loadmat
import xarray as xr
import numpy as np
from calendar import monthrange

class SEBS_monthly(XarrayBase):
    outofdb=True
    scheme="prec_evap"
    groupby="time"
    writeoutofdb=False
    def pull(self):
        auth=self.conf.authCred("sebs")
        downdir=self.cacheDir()
        files=["EB-ET/EB-ET_v2/Global_land_monthly_ET_V2.rar","Latitude.mat","Longitude.mat"]
        ftproot="ftp://210.72.14.198/"
        for f in files:
            ftpfile=ftp(ftproot+f,auth=auth,lastmod=datetime(2022,6,1))
            uri,updated=ftpfile.download(downdir,check=True)
        
        
        
    def convert2zarr(self,tarname):

        #load latitude and longitude 
        ddir=self.cacheDir()
        lonlat=loadmat(os.path.join(ddir,"Longitude.mat"))
        loadmat(os.path.join(ddir,"Latitude.mat"),mdict=lonlat)

        #start creating a basic xarray dataset
        dsbase=xr.Dataset(coords=dict(lon=(["lon"],lonlat['Longitude'][0,:]),lat=(["lat"],lonlat['Latitude'][:,0])))
        #NOTE: although the download ends with rar it is NOT A RAR but a TAR ARCHIVE!!!
                     
        tf = tarfile.open(tarname)
        appdim=None
        for mem in tf.getmembers():
            slurplog.info(f"Converting {mem.name} to zarr")
            mat=np.ma.masked_equal(loadmat(tf.extractfile(mem))['ETm'],0.0)
            mat.set_fill_value(np.nan)
            
            #extract the time centered at the 15th of the month
            time=datetime.strptime(mem.name[5:11]+"15","%Y%m%d")
            ds=dsbase.assign_coords(time=[time])
            ds["ETm"]=(["time","lat","lon"],np.expand_dims(mat,0))
            #note the scaling below also includes a scaling of 1/10 as mentioned here:https://data.tpdc.ac.cn/en/data/df4005fb-9449-4760-8e8a-09727df9fe36/)
            mmmon_kgsecm2=1e-1/(86400*monthrange(time.year,time.month)[1])
            ds["ETm"]=ds.ETm*mmmon_kgsecm2
            #add CF atributes
            cfadd_global(ds,title="SEBSv2 Evapotranspiration estimates",references="https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2020JD032873",source=f"Geoslurp class {self.__class__.__name__}")
            cfadd_standard_name(ds.ETm,"water_evapotranspiration_flux")
            # cfencode_time(ds.time)
            cfadd_coord(ds.lon,'X',standard_name='longitude')
            cfadd_coord(ds.lat,'Y',standard_name='latitude')
            if appdim:
                ds.to_zarr(self.xarfile,append_dim=appdim)
            else:

                ds.to_zarr(self.xarfile,mode='w')
                appdim="time"
                     
            
    
    def register(self):
        self.xarfile=os.path.join(self.dataDir(),"Global_land_monthly_ET_V2.zarr")
        tarar=os.path.join(self.cacheDir(),"Global_land_monthly_ET_V2.rar")
        if not os.path.isdir(self.xarfile):
            self.convert2zarr(tarar)
        
        super().register()


geoslurpCatalogue.addDataset(SEBS_monthly)
