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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dataset.RasterBase import RasterBase
from geoslurp.datapull.http import Uri as http
from datetime import datetime
import os
from zipfile import ZipFile
from geoslurp.config.slurplogger import slurplogger
from geoslurp.config.catalogue import geoslurpCatalogue
# from geoalchemy2.types import Raster
# from sqlalchemy.ext.declarative import declared_attr, as_declarative
# from sqlalchemy import Column, Integer, String, MetaData
# from geoslurp.datapull import findFiles
# from geoslurp.datapull.uri import UriFile
from sqlalchemy import func

# @as_declarative(metadata=MetaData(schema='dem'))
# class ArcticRasterTBase(object):
#     @declared_attr
#     def __tablename__(cls):
#         #strip of the 'Table' from the class name
#         return cls.__name__[:-5].lower()
#     id = Column(Integer, primary_key=True)
#     tile=Column(String)
#     rast=Column(Raster)

class Arcticdemindex(OGRBase):
    scheme='dem'
    targetsrid=3413
    filebase="ArcticDEM_Tile_Index_Rel7"
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),'extract',self.filebase+".shp")

        if "ArcticDEMversion"  in self._dbinvent.data:
            self._dbinvent.data["ArcticDEMversion"]=tuple(self._dbinvent.data["ArcticDEMversion"])
        else:
            self._dbinvent.data["ArcticDEMversion"] = (7, 0)

    def pull(self):
        """Pulls the shapefile layers from the server"""
        zipf=http("http://data.pgc.umn.edu/elev/dem/setsm/ArcticDEM/indexes/"+self.filebase+".zip",lastmod=datetime(2018,9,26))

        #download the zip shapefiles
        downloaddir=self.cacheDir()
        uri,upd=zipf.download(downloaddir,check=True)
        zipd=os.path.join(downloaddir,'extract')
        if not os.path.exists(zipd):
            #unzip the goodies
            with ZipFile(uri.url,'r') as zp:
                slurplogger().info("Unzipping %s"%(uri.url))
                zp.extractall(zipd)



# def ArcticDEMMetaExtractor(uri):
#     slurplogger().info("Extracting info from raster: %s"%(uri.url))
#     with open(uri.url,'rb') as fid:
#         fbytes=fid.read()
#     meta={"rast":func.ST_FromGDALRaster(fbytes)}
#     return meta

class ArcticDemRasterBase(RasterBase):
    """"Base class to download/register Arctic DEM Tiff rasters"""
    scheme="dem"
    res=None
    rasterfile=None
    rastregex = '.*\.tif$'
    srid=3413

    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.rasterfile="arcticdem_mosaic_"+self.res+"_v3.0.tif"
        self._dbinvent.data["Description"]="ArcticDEM raster table"

    def pull(self, intersect=None):
        # download the entire mosaic domain in one tif
        if self.res in ['1km','500m','100m']:
            rasteruri=http("http://data.pgc.umn.edu/elev/dem/setsm/ArcticDEM/mosaic/v3.0/"+self.res+"/"+self.rasterfile,lastmod=datetime(2018,9,26))
            rasterfileuri,upd=rasteruri.download(self.srcdir,check=False)

        #download only those tiles which are needed


def getArcticDems(conf):
    out=[]
    for res in ['1km', '500m', '100m']:
        out.append(type("arcticdem_mosaic_"+res+"_v3", (ArcticDemRasterBase,), {"res":res,"tiles":[100,100]}))
        # out.append(type("arcticdem_mosaic_"+res+"_v3", (ArcticDemRasterBase,), {"res":res}))

    return out


#register datasets
geoslurpCatalogue.addDataset(Arcticdemindex)
geoslurpCatalogue.addDatasetFactory(getArcticDems)

