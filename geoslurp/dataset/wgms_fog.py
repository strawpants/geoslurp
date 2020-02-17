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


from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from zipfile import ZipFile
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.dataset.pandasbase import PandasBase
from geoalchemy2 import Geography,WKTElement
from sqlalchemy.types import String
from shapely.wkt import dumps as wktdump
from shapely.geometry import Point
import re

scheme='cryo'

def pullFoG(downloaddir):
    """Pulls various GRDC datasets from the webdav folder"""
    fogSource=http("http://www.wgms.ch/downloads/DOI-WGMS-FoG-2018-06.zip")
    urif,upd=fogSource.download(downloaddir)
    if upd:
        with ZipFile(urif.url,'r') as zp:
                zp.extractall(downloaddir)


geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

class wgms_fogBase(PandasBase):
    scheme=scheme
    ftype='csv'

    # dtypes={"geom":geoPointtype}
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not self._dbinvent.cache:
                self._dbinvent.cache=self.conf.getDir(self.scheme,"CacheDir",subdirs='WGMSFOG')
        self.pdfile=os.path.join(self._dbinvent.cache,'DOI-WGMS-FoG-2018-06',self.pdfile)

    def pull(self):
        """pulls the WGMS FOG zip file and extracts it """
        pullFoG(self._dbinvent.cache)
    
    def modify_df(self,df):
        """Adds a geometry column extracted from the other columns"""
        self.dtypes={}
        #change all panda object types to ocnvert to a Sqlalcehmy String
        for ky,typ in df.dtypes.items():
            if type(object) == typ:
                self.dtypes[ky]=String
            
             
        if re.search('A-GLACIER.csv',self.pdfile):
            #extract longitude and latitude
            self.dtypes["geom"]=geoPointtype
            df.LONGITUDE=df.LONGITUDE.fillna(0)
            df.LATITUDE=df.LATITUDE.fillna(0)

            df['geom']=[WKTElement(wktdump(Point(lon,lat,0)),srid=4326,extended=True) for lon,lat in zip(df.LONGITUDE,df.LATITUDE)] 
        
       
        if re.search('EEE-MASS-BALANCE-POINT.csv',self.pdfile):
            #extract longitude and latitude
            df.POINT_LON=df.POINT_LON.fillna(0)
            df.POINT_LAT=df.POINT_LAT.fillna(0)
            df.POINT_ELEVATION=df.POINT_ELEVATION.fillna(0)
            df['geom']=[WKTElement(wktdump(Point(lon,lat,h)),srid=4326,extended=True) for lon,lat,h in zip(df.POINT_LON,df.POINT_LAT,df.POINT_ELEVATION)] 

            self.dtypes["geom"]=geoPointtype
        
        
        return df



# Factory method to dynamically create classes
def FOGClassFactory(fileName):
    splt=fileName.lower().replace('-','_').split(".")
    return type(splt[0], (wgms_fogBase,), {"pdfile":fileName,"encoding":"iso-8859-1"})


def getFOGDsets(conf):
    """Automatically create all classes contained within the WGMS FOG csv tables"""

    csvfiles=['WGMS-FoG-2018-06-AA-GLACIER-ID-LUT.csv','WGMS-FoG-2018-06-A-GLACIER.csv','WGMS-FoG-2018-06-B-STATE.csv','WGMS-FoG-2018-06-C-FRONT-VARIATION.csv','WGMS-FoG-2018-06-D-CHANGE.csv','WGMS-FoG-2018-06-EEE-MASS-BALANCE-POINT.csv','WGMS-FoG-2018-06-EE-MASS-BALANCE.csv','WGMS-FoG-2018-06-E-MASS-BALANCE-OVERVIEW.csv','WGMS-FoG-2018-06-F-SPECIAL-EVENT.csv','WGMS-FoG-2018-06-R-RECONSTRUCTION-SERIES.csv','WGMS-FoG-2018-06-RR-RECONSTRUCTION-FRONT-VARIATION.csv']
    
    out=[FOGClassFactory(name) for name in csvfiles]
    
    return out



geoslurpCatalogue.addDatasetFactory(getFOGDsets)
