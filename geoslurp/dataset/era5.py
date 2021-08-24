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


from geoslurp.dataset import DataSet

from geoslurp.config.slurplogger import slurplogger
import cdsapi
import os


class ERA5Base(DataSet):
    """Provides a Base class from which subclasses can inherit to download a subset of the data per area"""
    scheme="atmo"
    dset='reanalysis-era5-pressure-levels-monthly-means'
    productType='monthly_averaged_reanalysis'
    yrstart=2000
    yrend=2000
    variables={}
    areas={}
    time="00:00"
    
    def appendAreaRequest(self,name,area):
        bb=area.envelope
        import pdb;pdb.set_trace()
        self.areas[name]=bb

    def pull(self):

        if not os.path.exists(os.path.join(os.path.expanduser(),".cdsapirc")):
            raise RuntimeError("Before using the cdsapi please visit https://cds.climate.copernicus.eu/api-how-to to obtain a token and setup your ~/.cdsapirc file")
        dout=self.dataDir()
        #start a client
        # c = cdsapi.Client()
        for name,area in self.areas.items():
            fout=os.path.join(dout,self.dset+"_"+name+".nc")
            if os.path.exists(fout):
                slurplogger().info(f"Already downloaded ERA5 data for area {name}")
                continue
            requestdict=getRequestDict(area)
            slurplogger().info(f"Downloading ERA5 for {name}")
            # c.retrieve(self.dset,requestdict,fout)

    def getRequestDict(self,area):
        """Builds a dictionary for the cdsapi
        :param area (shapely geometry) geometry which will be used to compute the bounding box to download data for"""
        reqdict={
                'format': 'netcdf',
                'product_type': self.productType,
                'variable': self.variables,
                'pressure_levels':self.plevels,
                'year': [f"{yr}" for yr in range(self.yrstart,self.yrend+1)],
                'month':[f"{mn:02d}" for mn in range(1,13)],
                'time': self.time,
                'area': self.area}
        return reqdict

    def register(self):
        pass



