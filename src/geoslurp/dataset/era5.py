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
from geoslurp.datapull.cds import Cds
from geoslurp.config.slurplogger import slurplogger
import os
import numpy as np
from geoalchemy2 import Geography
from shapely.geometry import Polygon
from shapely.wkt import dumps as wktdumps
from netCDF4 import Dataset as ncDset
from netCDF4 import num2date
import time
from urllib.error import HTTPError
from geoslurp.dataset.cdsbase import CDSBase

envelopType = Geography(geometry_type="POLYGON", srid='4326')


class ERA5Base(CDSBase):
    """Provides a Base class from which subclasses can inherit to download a subset of the data per area"""
    scheme="atmo"
    resource='reanalysis-era5-pressure-levels-monthly-means'
    productType='monthly_averaged_reanalysis'
    yrstart=2000
    yrend=2000
    variables=[]
    time="00:00"

    def __init__(self,dbconn):
        super().__init__(dbconn)

    def appendRequest(self,name,geomshape):
        """Builds a dictionary for the cdsapi
        :param geomshape (shapely geometry) geometry which will be used to compute the bounding box to download data for"""
        reqdict=self.getDefaultDict(geomshape)
        if self.plevels:
            reqdict['pressure_level']=self.plevels
        reqdict['year']= [f"{yr}" for yr in range(self.yrstart,self.yrend+1)]
        reqdict['month']=[f"{mn:02d}" for mn in range(1,13)]
        reqdict['time']=self.time
        
        self.reqdicts[name]=reqdict


    def metaExtractor(self,ncuri):
        name=os.path.basename(ncuri.url).split('_')[-1][0:-3]
        ncid=ncDset(ncuri.url)
        data={"dimensions":{ky:val.size for ky,val in ncid.dimensions.items()},
                "variables":{ky:{"long_name":val.long_name,"dimensions":val.dimensions} for ky,val in ncid.variables.items()}}
        tstart=num2date(ncid["time"][0],units=ncid['time'].units,only_use_cftime_datetimes=False)
        tend=num2date(ncid["time"][-1],units=ncid['time'].units,only_use_cftime_datetimes=False)
        latmin=np.min(ncid["latitude"])
        latmax=np.max(ncid["latitude"])
        lonmin=np.min(ncid["longitude"])
        lonmax=np.max(ncid["longitude"])
        bbox=Polygon([(lonmin,latmin),(lonmin,latmax),(lonmax,latmax),(lonmax,latmin)])
        return {"name":name,"lastupdate":ncuri.lastmod,"tstart":tstart,"tend":tend,"uri":ncuri.url,"data":data,"geom":wktdumps(bbox)}

