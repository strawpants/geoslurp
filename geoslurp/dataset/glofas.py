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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2021


from sqlalchemy import Column, Integer, String, DateTime,JSON
from geoslurp.dataset import DataSet
from geoslurp.datapull import findFiles, UriFile
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
import os
import numpy as np
from shapely.geometry import Polygon
from shapely.wkt import dumps as wktdumps
from geoslurp.dataset.cdsbase import CDSBase
from geoslurp.dataset.RasterBase import RasterBase
import xarray as xr
from datetime import datetime
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.types.numpy import np_to_datetime
from copy import deepcopy

class GloFASBase(CDSBase):
    """Provides a Base class from which subclasses can inherit to download a subset of the data per area"""
    scheme="hydro"
    resource='cems-glofas-historical'
    productType='consolidated'
    yrstart=2000
    yrend=2000
    variables='river_discharge_in_the_last_24_hours'
    oformat='grib'
    res=0.1 #degrees
    def __init__(self,dbconn):
        super().__init__(dbconn)

    def appendRequest(self,name,geomshape):
        """Builds a dictionary for the cdsapi
        :param geomshape (shapely geometry) geometry which will be used to compute the bounding box to download data for"""
        reqdict=self.getDefaultDict(geomshape)
        reqdict['system_version']='version_3_1'
        reqdict['hydrological_model']='lisflood'
        reqdict['hyear']= [f"{yr}" for yr in range(self.yrstart,self.yrend+1)]
        reqdict['hmonth']=['january','february','march','april','may','june','july','august', 'september','october','november','december']
        reqdict['hday']=[f"{mn:02d}" for mn in range(1,32)]
        
        #loop over the requested years (need to split this up to prevent too lrage requests
        for yr in range(self.yrstart,self.yrend+1):
            reqdictcp=deepcopy(reqdict)
            reqdictcp["hyear"]=f"{yr}"
            name_yr=f"{name}_{yr}"
            self.reqdicts[name_yr]=reqdictcp


    def metaExtractor(self,uri):
        name="".join(os.path.basename(uri.url).split('_')[-2:])[0:-4]
        ds=xr.open_dataset(uri.url,engine='cfgrib')
        data={"dimensions":{ky:val for ky,val in ds.dims.items()},"variables":{ky:{"long_name":val.attrs["long_name"],"dimensions":val.dims} for ky,val in ds.variables.items()}}

        tstart=np_to_datetime(ds.time.values[0])
        tend=np_to_datetime(ds.time.values[-1])
        latmin=ds.latitude.min().values
        latmax=ds.latitude.max().values
        lonmin=ds.longitude.min().values
        lonmax=ds.longitude.max().values
        bbox=Polygon([(lonmin,latmin),(lonmin,latmax),(lonmax,latmax),(lonmax,latmin)])
        return {"name":name,"lastupdate":uri.lastmod,"tstart":tstart,"tend":tend,"uri":uri.url,"data":data,"geom":wktdumps(bbox)}


class GloFASUpArea(RasterBase):
    """Class which downloads and registers the auxiliary uparea file"""
    regularblocking=True
    scheme="hydro" 
    
    def pull(self):
        upsrc=http("https://confluence.ecmwf.int/download/attachments/143039724/upArea.nc",lastmod=datetime(2021,11,17))
        #download to cache only (will be in db raster)
        urif,upd=upsrc.download(self.srcdir,check=True)



geoslurpCatalogue.addDataset(GloFASUpArea)



