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


from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.config.slurplogger import slurplogger
from geoslurp.datapull.webdav import Crawler as webdav
from zipfile import ZipFile
import os
from geoslurp.config.register import geoslurpregistry
from geoslurp.dataset.pandasbase import PandasBase
from geoslurp.dataset.OGRBase import OGRBase
from geoalchemy2 import Geography,WKTElement
from shapely.wkt import dumps as wktdump
from shapely.geometry import Point



def pullGRDCGIS(downloaddir,auth):

    gissource=webdav("https://uni-bonn.sciebo.de/public.php/webdav",auth=auth,pattern="GIS_layers\.zip")
    zipd=os.path.join(downloaddir,"extract")
    for uri in gissource.uris():
        urif,upd=uri.download(downloaddir,check=True)
        #unzip if newly updated
        if upd:
            with ZipFile(urif.url,'r') as zp:
                    zp.extractall(zipd)


scheme='grdc'

class grdc_gis_base(OGRBase):
    """Base class for the shapefiles from the grdc station data. Note """
    scheme=scheme
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not self._dbinvent.cache:
                self._dbinvent.cache=self.conf.getDir(self.scheme,"CacheDir")
        self.ogrfile=os.path.join(self.cacheDir(),'extract',self.filename)
    
    def pull(self):
        """Pulls the GIS_Layers.zip file from the web and unzip it in the cache"""
        try:
            cred=self.conf.authCred("grdcgis")
        except:
            raise RuntimeError("No Authentification data found. The GRDC GIS data is unfortunately not publically available, please contact roelof@wobbly.earth") 
        
        pullGRDCGIS(self.cacheDir(),cred)

class grdc_stations(PandasBase):
    scheme=scheme
    ftype='excel'
    dtypes={"geom":Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)}
    pdfile='grdc_stations.xlsx'
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not self._dbinvent.cache:
                self._dbinvent.cache=self.conf.getDir(self.scheme,"CacheDir")
        self.pdfile=os.path.join(self.cacheDir(),'extract',self.pdfile)

    def pull(self):
        """pulls the station catalogue as excel file """
        downloaddir=self.cacheDir()
        urif,upd=ftp("ftp://ftp.bafg.de/pub/REFERATE/GRDC/catalogue/grdc_stations.zip").download(downloaddir)
        if upd:
            with ZipFile(urif.url,'r') as zp:
                    zp.extractall(os.path.join(downloaddir,'extract'))
    
    def modify_df(self,df):
        """Adds a geometry column and converts day,month,year column to proper dates"""

        #make a geometry column with points
        df.altitude=df.altitude.replace(-999,0)
        df['geom']=[WKTElement(wktdump(Point(lon,lat,h)),srid=4326,extended=True) for lon,lat,h in zip(df.long,df.lat,df.altitude)] 
        

        # gdf = gpd.GeoDataFrame(
                    # df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
        
        return df
# class RGICSVBase(CSVBase):
    # """Base class for the CSV files from the Randalph Glacier Inventory Datasets. They are all quite similar so letting them inherit from a baseclass
    # seems reasonable
    # """
    # scheme='cryo'
    # lookup=csvLookup()
    # filename=''
    # def __init__(self,dbconn):
        # super().__init__(dbconn)
        # if "RGIversion"  in self._dbinvent.data:
            # self._dbinvent.data["RGIversion"]=tuple(self._dbinvent.data["RGIversion"])
        # else:
            # self._dbinvent.data["RGIversion"] = (0, 0)
        
        # if not self._dbinvent.cache:
                # self._dbinvent.cache=self.conf.getDir(self.scheme,"CacheDir")
        
        # self.csvfile=os.path.join(self.cacheDir(),'extract',self.filename)

    # def pull(self):
        # """Pulls the entire RGI archive from the web and stores it in a cache"""
        # version,updated=pullRGI(self.cacheDir(),self._dbinvent.data['RGIversion'])
        # if updated:
            # self._dbinvent.data["RGIversion"] = version
        # self.updateInvent(False)


# Factory method to dynamically create classes
def GRDCGISClassFactory(fileName):
    splt=fileName.split(".")
    return type(splt[0], (grdc_gis_base,), {"filename":fileName,"gtype":"GEOMETRY","swapxy":True})


# def RGICSVClassFactory(fileName,hskip=0):
    # splt=fileName.split(".")
    # return type(splt[0], (RGICSVBase,), {"filename":fileName,"hskip":hskip})


def getGRDCDsets(conf):
    """Automatically create all classes contained within the GRDC tables"""
    GISshapes=['GRDC_405_basins_from_mouth.shp','GRDC_687_rivers.shp','GRDC_687_rivers_class.shp','GRDC_lakes_join_rivers.shp','grdc_basins_smoothed.shp']
    
    out=[GRDCGISClassFactory(name) for name in GISshapes]

    return out



geoslurpregistry.registerDatasetFactory(getGRDCDsets)
geoslurpregistry.registerDataset(grdc_stations)
