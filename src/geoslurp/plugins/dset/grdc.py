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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2024


# from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.config.slurplogger import slurplog
# from geoslurp.datapull.webdav import Crawler as webdav
from zipfile import ZipFile
import os
# from geoslurp.dataset.pandasbase import PandasBase
from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.view.viewBase import TView 
from geoslurp.datapull.uri import UriFile,findFiles
from sqlalchemy import MetaData,text
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import JSONB,TIMESTAMP
from shapely.geometry import Point
import gzip
from datetime import datetime,timedelta

from geoslurp.datapull.http import Uri as http
from geoslurp.datapull.uri import setFtime

import requests
import pandas as pd
import geopandas as gpd
import json
import numpy as np

schema='grdcv2'

class grdc_stations(DataSet):
    schema=schema
    def __init__(self,dbconn):
        super().__init__(dbconn)

        self.jsonfile=os.path.join(self.cacheDir(),"grdc_sample_records.json")

    def pull(self):
        """Retrieve the station catalogue as json"""
        url="https://portal.grdc.bafg.de/grdc/grdc_sample_records.json"
        uri,updated=http(url,lastmod=datetime.now()).download(self.cacheDir(),check=True,gzip=True)
        #set modification time (one day in the future to prevent spurious downloads
        if updated:
            setFtime(uri.url,datetime.now()+timedelta(days=1))
    
    def register(self):
        file=self.cacheDir("grdc_sample_records.json.gz")
        with gzip.open(file,'r') as gzid:
            df=pd.read_json(gzid)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat,crs="EPSG:4326"))
        gdf.to_postgis(self.name,self.db.dbeng,schema=self.schema,if_exists='replace')



class grdc_stations_monthly(TView):
    schema=schema
    prefix="monthly"
    qry=f"SELECT grdc_no,provider_id,wmo_reg,sub_reg,nat_id,river,station,country,area,altitude,ds_stat_no,lta_discharge,r_volume_yr,l_im_yr,r_height_yr,region_name,subregion_name,ocean,river_basin, make_timestamp(m_start::int,m_start_month::int,m_start_day::int,0,0,0) AS tstart, make_timestamp(m_end::int,m_end_month::int,m_end_day::int,23,59,59) AS tend, geometry FROM {grdc_stations.stname()} WHERE m_start IS NOT NULL AND m_end IS NOT NULL"

class grdc_stations_daily(TView):
    schema=schema
    prefix="daily"
    qry=f"SELECT grdc_no,provider_id,wmo_reg,sub_reg,nat_id,river,station,country,area,altitude,ds_stat_no,lta_discharge,r_volume_yr,l_im_yr,r_height_yr,region_name,subregion_name,ocean,river_basin, make_timestamp(d_start::int,d_start_month::int,d_start_day::int,0,0,0) AS tstart, make_timestamp(d_end::int,d_end_month::int,d_end_day::int,23,59,59) AS tend, geometry FROM {grdc_stations.stname()} WHERE d_start IS NOT NULL AND d_end IS NOT NULL"


class grdc_statbasins(DataSet):
    schema=schema
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.filebase="statbas_json_zip.zip" 
    
    def pull(self):
        """Download polygons of upstream areas of the discharge stations"""
        url="https://grdc.bafg.de/SharedDocs/ExterneLinks/GRDC/statbas_json_zip.zip?__blob=publicationFile"
        
        uri,updated=http(url,lastmod=datetime(2021,8,17)).download(self.cacheDir(),check=True,outfile=self.filebase)
        

    def register(self):
        """"Register all downloaded polygons"""
        self.dropTable()
        encoding='iso-8859-1' #default for shapefiles (also work here)
        zpfile=os.path.join(self.cacheDir(),self.filebase)
        with ZipFile(zpfile,'r') as zpid:
            for member in zpid.namelist():
                if member.endswith(".geojson"):
                    with zpid.open(member,'r') as fid:
                        slurplog.info(f"Adding {member}")
                        gdf=gpd.read_file(fid,engine="pyogrio",encoding=encoding)
                        #We pack all data in a single table but add the contintent flag
                        gdf['continent']=member.split("_")[-2]
                        gdf.to_postgis(self.name,self.db.dbeng,schema=self.schema,if_exists='append')

@as_declarative(metadata=MetaData(schema=schema))
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
    data=Column(JSONB)

class grdc_DSBase(DataSet):
    """Holdings of the daily and monthly GRDC discharge files"""
    version=(1,0,0)
    schema=schema
    view=None # replace with either monthly or daily view of station table
    def __init__(self,dbcon):
        super().__init__(dbcon)
        if self.view.vname().endswith("monthly"):
            self.tspathend="Q/Merge.Months"
        else:
            self.tspathend="Q/Day.Cmd"

    def renewHttpSession(self):
        #Login to get cookies
        ses=requests.Session()
        jsonlogin={"userName":"PublicUser","password":"PublicUser123"}
        loginurl="https://portal.grdc.bafg.de/KiWebPortal/rest/auth/login"
        loginresp=ses.post(loginurl,json=jsonlogin)
        if loginresp.status_code != 201:
            breakpoint()
            raise RuntimeError("cannot get cookies from grdc portal")
        ses.headers["User-Agent"]="Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
        ses.headers["Accept"]="application/json"
        ses.headers["Content-Type"]="application/json"
        ses.headers["Sec-Fetch-Dest"]="empty"
        ses.headers["Sec-Fetch-Mode"]="cors"
        ses.headers["Sec-Fetch-Site"]="same-origin"        
        return ses

    def pull(self):
        """Pulls the time series from the grdc portal"""
        view=self.view(self.db)
        if not view.exists():
            # register view if it does not exist
            slurplog.info(f"Registering {view.name} view")
            view.register()
        ddir=self.cacheDir()
        ses=None
        
        tsurl="https://portal.grdc.bafg.de/KiWebPortal/rest/wiski7/dataSources/grdc/timeSeries/data"
        with self.db.dbeng.connect() as conn:
            res=conn.execute(text(f"SELECT tstart,tend,grdc_no,provider_id FROM {view.svname()}"))
            conn.commit()

            for station in res:
                cacheuri=UriFile(os.path.join(ddir,f"{station.grdc_no}_{view.prefix}_Q.json.gz"))
                if cacheuri.lastmod > station.tend:
                    slurplog.info(f"{cacheuri.url} is downloaded already, skipping download")
                    continue
                #create paramers for the data
                tsparams={"from":f"{station.tstart.isoformat()}Z","to":f"{station.tend.isoformat()}Z","returnfields":"Timestamp,Value","ts_path":f"{station.provider_id}/{station.grdc_no}/{self.tspathend}","valuesasstring":True}
                
                if ses is None:
                    ses=self.renewHttpSession()
                
                # download the data
                slurplog.info(f"Downloading {cacheuri.url}")
                ts_result=ses.get(tsurl,params=tsparams)
                if ts_result.status_code != 200:
                    slurplog.info("Failure to load data from server, renewing session")
                    #ATTEMPT 2 try renewing the session, because the cookie may have expired
                    ses=self.renewHttpSession()
                    ts_result=ses.get(tsurl,params=tsparams)
                    if ts_result.status_code != 200:
                        # definitive error, abort
                        raise RuntimeError("Unable to download data from server after second attempt, quitting")

                with gzip.open(cacheuri.url,'wb') as fid:
                    fid.write(ts_result.content)



    def register(self):
        slurplog.info("Building file list..")
        files=[UriFile(file) for file in findFiles(self.cacheDir(),'.*json.gz',self._dbinvent.lastupdate)]

        if len(files) == 0:
            slurplog.info(f"No update needed for {self.stname()}")
            return
        check_col=["grdc_no"] 
        for uri in files:
            with gzip.open(uri.url,'rb') as fid:
                entry=json.loads(fid.read())
            
            if len(entry) != 1:
                raise RuntimeError("can't handle multiple JSON rows yet")
            
            slurplog.info(f"registering {os.path.basename(uri.url)}")
            grdcid=int(os.path.basename(uri.url).split("_")[0])

            time=[val[0] for val in entry[0]['data']]
            Q=[val[1] for val in entry[0]['data']]
            tstart=datetime.fromisoformat(time[0])
            tend=datetime.fromisoformat(time[-1])

            entry={"grdc_no":grdcid,"lastupdate":uri.lastmod,"tstart":tstart,"tend":tend,"data":{"time":time,"Q":Q}}

            self.upsertEntry(entry,check_col)
        
        self.updateInvent()

class grdc_mrbBase(OGRBase):
    filebase="mrb_shp_zip.zip"
    schema=schema
    gtype = "GEOMETRY"
    cached="GRDC_MRB"
    swapxy=True
    url="https://www.bafg.de/SharedDocs/ExterneLinks/GRDC/mrb_shp_zip.zip?__blob=publicationFile"
    testfile="readme_mrb.txt"
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.setCacheDir(self.conf.getCacheDir(self.schema,self.cached))
        self.layerregex=self.__class__.__name__
        self.ogrfile=self.cacheDir()
    def pull(self):
        cdir=self.cacheDir()
        
        uri,updated=http(self.url,lastmod=datetime(2021,11,17)).download(cdir,outfile=self.filebase,check=True)
        #extract the data
        if not os.path.exists(os.path.join(cdir,self.testfile)):
            with ZipFile(uri.url,'r') as zid:
                for member in zid.namelist():
                    slurplog.info(f"unzipping {member}")
                    zid.extract(member,path=cdir)

class grdc_wmoBase(grdc_mrbBase):
    filebase="wmobb_shp_zip.zip"
    schema=schema
    gtype = "GEOMETRY"
    cached="GRDC_WMOBB"
    swapxy=True
    url="https://grdc.bafg.de/SharedDocs/ExterneLinks/GRDC/wmobb_shp_zip.zip?__blob=publicationFile"
    testfile="readme_wmobb.txt"


def getGRDCDsets(conf):
    grdcdset=[grdc_stations,grdc_statbasins]
    # add daily and monthly data tables
    for clsName in ["grdc_q_monthly","grdc_q_daily"]:
        if clsName.endswith("monthly"):
            view=grdc_stations_monthly
        else:
            view=grdc_stations_daily
        qtable=type(clsName +"Table", (grdcTBase,), {})
        grdcdset.append(type(clsName, (grdc_DSBase,), {"table":qtable,"view":view}))
    
    # add major river basins of the world shape layers
    mrblayers=["mrb_basins","mrb_rivernames","mrb_rivers","mrb_named_rivers","mrb_river_names"]
    wmobblayers=["wmobb_basin","wmobb_rivers","wmobb_rivernames"]
    for st,nd in enumerate(range(1,11)):
        mrblayers.append(f"mrb_rivnets_Q{st:02d}_{nd:02d}")
        wmobblayers.append(f"wmobb_rivnets_Q{st:02d}_{nd:02d}")
    
    for layer in mrblayers:
        grdcdset.append(type(layer,(grdc_mrbBase,),{}))
    
    for layer in wmobblayers:
        grdcdset.append(type(layer,(grdc_wmoBase,),{}))

    return grdcdset

        
def getGRDCviews():
    return [grdc_stations_daily,grdc_stations_monthly]

