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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020


from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.config.slurplogger import slurplogger
from geoslurp.datapull.webdav import Crawler as webdav
from zipfile import ZipFile
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.dataset.pandasbase import PandasBase
from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.datapull import findFiles,UriFile
from geoalchemy2 import Geography,WKTElement,WKBElement
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import TIMESTAMP
from shapely.wkt import dumps as wktdump
from shapely.wkb import dumps as wkbdump
from shapely.geometry import Point
import re
import gzip
from datetime import datetime

scheme='grdc'

def pullGRDC(downloaddir,auth,pattern,unzip=True):
    """Pulls various GRDC datasets from a webdav folder"""
    gissource=webdav(auth.url,auth=auth,pattern=pattern)
    for uri in gissource.uris():
        urif,upd=uri.download(downloaddir,check=True)
        #unzip if newly updated
        if upd and unzip:
            with ZipFile(urif.url,'r') as zp:
                    zp.extractall(downloaddir)

class grdc_gis_base(OGRBase):
    """Base class for the shapefiles from the grdc station data. Note """
    scheme=scheme
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not self._dbinvent.cache:
                self._dbinvent.cache=self.conf.getCacheDir(self.scheme,subdirs='GIS_layers')
        self.ogrfile=os.path.join(self._dbinvent.cache,self.filename)
    
    def pull(self):
        """Pulls the GIS_Layers.zip file from the web and unzip it in the cache"""
        try:
            cred=self.conf.authCred("grdcgis")
        except:
            raise RuntimeError("No Authentification data found. The GRDC GIS data is unfortunately only available after agreeing with the grdc user policy, please visit https://www.bafg.de/GRDC/EN/04_spcldtbss/43_GRfN/refDataset_node.html") 
        
        pullGRDC(self._dbinvent.cache,cred,pattern="GIS_layers\.zip")

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)
class grdc_catalogue(PandasBase):
    scheme=scheme
    ftype='excel'
    dtypes={"geom":geoPointtype}
    pdfile='GRDC_Stations.xlsx'
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not self._dbinvent.cache:
                self._dbinvent.cache=self.conf.getCacheDir(self.scheme)
        self.pdfile=os.path.join(self.cacheDir(),self.pdfile)

    def pull(self):
        """pulls the station catalogue as excel file """
        downloaddir=self.cacheDir()
        urif,upd=ftp("ftp://ftp.bafg.de/pub/REFERATE/GRDC/catalogue/grdc_stations.zip").download(downloaddir)
        if upd:
            with ZipFile(urif.url,'r') as zp:
                    zp.extractall(downloaddir)

        #also download the time series in the data directory
    
    def modify_df(self,df):
        """Adds a geometry column TODO: converts day,month,year column to proper dates"""

        #make a geometry column with points
        df.altitude=df.altitude.replace(-999,0)
        df['geom']=[WKTElement(wktdump(Point(lon,lat,h)),srid=4326,extended=True) for lon,lat,h in zip(df.long,df.lat,df.altitude)] 
        

        # gdf = gpd.GeoDataFrame(
                    # df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
        
        return df



@as_declarative(metadata=MetaData(schema=scheme))
class grdcTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id=Column(Integer,primary_key=True)
    grdc_no=Column(Integer,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    area=Column(Float)
    altitude=Column(Float)
    river=Column(String)
    statname=Column(String)
    country=Column(String)
    nextstation=Column(Integer)
    uri=Column(String)
    remarks=Column(String)
    geom=Column(geoPointtype)

def GRDCmetaExtractor(uri):
    encoding='iso-8859-1'
    meta={}
    headerregex=re.compile(b"^#")
    dataregex=re.compile(b"^[0-9]")
    header={}
    epoch=[]
    slurplogger().info("Extracting meta info from: %s"%(uri.url))
    with gzip.open(uri.url,'r') as gzid:
        for ln in gzid:
            if headerregex.search(ln):
                #parse header
                lnspl=ln[1:].split(b":",1)
                if len(lnspl) == 2:
                    ky=lnspl[0].lstrip(b" ").rstrip(b" ").decode(encoding)
                    val=lnspl[1].lstrip(b" ").rstrip(b"\r\n ").decode(encoding)
                    if not val:
                        #get the nextline as a value
                        val=gzid.readline()[1:].lstrip(b" ").rstrip(b"\r\n ").decode(encoding)
                    header[ky]=val
                continue
            if dataregex.search(ln):
                #parse data line
                lnspl=ln.decode(encoding).split(";")
                datestr=lnspl[0]+lnspl[1].replace("--:--",":00:00")
                epoch.append(datetime.strptime(datestr, '%Y-%m-%d:%H:%S'))
    
    nextdownstream=header["Next downstream station"]

    if "-" in nextdownstream:
        nextdownstream=None
    else:
        nextdownstream=int(nextdownstream)
    
    area=float(header["Catchment area (km²)"])
    if area == -999.0:
        area=None

    alt=float(header["Altitude (m ASL)"])
    if alt == -999.0:
        alt=None
        location=Point(float(header["Longitude (DD)"]),float(header["Latitude (DD)"]),0)
    else:
        location=Point(float(header["Longitude (DD)"]),float(header["Latitude (DD)"]),alt)
    # import pdb;pdb.set_trace()
    try:
        meta={"grdc_no":int(header["GRDC-No."]),
                "tstart":epoch[0],
                "tend":epoch[-1],
                "lastupdate":datetime.strptime(header["Last update"],"%Y-%m-%d"),
                "area":area,
                "altitude":alt,
                "river":header["River"],
                "statname":header["Station"],
                "country":header["Country"],
                "nextstation":nextdownstream,
                "uri":uri.url,
                "geom":WKTElement(wktdump(location),srid=4326,extended=True)}
                # "geom":WKBElement(wkbdump(location),srid=4326,extended=True)}
    except KeyError as e:
        import pdb;pdb.set_trace()
        raise(e)


    return meta

class grdc_DSBase(DataSet):
    """Holdings of the daily and monthly discharge files"""
    """GRDC river discharge holdings"""
    version=(0,0,0)
    scheme=scheme
    def __init__(self,dbcon):
        super().__init__(dbcon)
        if re.search("monthly",self.name):
            self.zipname="DATA_MON.ZIP"
        else:
            self.zipname="DATA_DAY.ZIP"

    def pull(self):
        try:
            cred=self.conf.authCred("grdcgis")
        except:
            raise RuntimeError("No Authentification data found. The GRDC data is unfortunately only available after agreeing with the grdc user policy, please visit https://www.bafg.de/GRDC/EN/04_spcldtbss/43_GRfN/refDataset_node.html") 
        #pull the data but rezip it with gzip to save space 
        pullGRDC(self.cacheDir(),cred,pattern=self.zipname,unzip=False)

        datadir=self.dataDir()
        #rezip data in the datadirectory
        with ZipFile(os.path.join(self.cacheDir(),self.zipname),'r') as zp:
            for member in zp.namelist():
                #open file and gzip it into the datadir
                with zp.open(member) as fid:
                    slurplogger().info("re-gzipping file %s"%member)
                    with gzip.open(os.path.join(datadir,member+".gz"),'wb') as gzid:
                            gzid.write(fid.read())

                     
    def register(self):
        slurplogger().info("Building file list..")
        files=[UriFile(file) for file in findFiles(self.dataDir(),'.*gz',self._dbinvent.lastupdate)]
        # import pdb;pdb.set_trace() 
        filesnew=self.retainnewUris(files)
        if len(filesnew) == 0:
            slurplogger().info("GRDC: No database update needed")
            return
        # filesnew=[UriFile(os.path.join(self.dataDir(),"4208270_Q_Month.txt.gz"))]
        #loop over files
        for uri in filesnew:
            meta=GRDCmetaExtractor(uri)
            self.addEntry(meta)
        
        self.updateInvent()


def GRDCClassFactory(clsName):
    table=type(clsName +"Table", (grdcTBase,), {})
    return type(clsName, (grdc_DSBase,), {"table":table})

# Factory method to dynamically create classes
def GRDCGISClassFactory(fileName):
    splt=fileName.split(".")
    return type(splt[0], (grdc_gis_base,), {"filename":fileName,"gtype":"GEOMETRY","swapxy":True})


def getGRDCDsets(conf):
    """Automatically create all classes contained within the GRDC tables"""
    GISshapes=['GRDC_405_basins_from_mouth.shp','GRDC_687_rivers.shp','GRDC_687_rivers_class.shp','GRDC_lakes_join_rivers.shp','grdc_basins_smoothed.shp']
    
    out=[GRDCGISClassFactory(name) for name in GISshapes]
    
    #also add the monthly and daily  datasets
    for name in ["grdc_monthly","grdc_daily"]:
        out.append(GRDCClassFactory(name))

    return out



geoslurpCatalogue.addDatasetFactory(getGRDCDsets)
geoslurpCatalogue.addDataset(grdc_catalogue)
