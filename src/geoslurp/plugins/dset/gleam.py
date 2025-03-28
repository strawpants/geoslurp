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

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.datapull.sftp import CrawlerSftp as crawler
from geoslurp.config.slurplogger import slurplog
from pathlib import Path
from geoslurp.tools.cf import cfadd_coord
from datetime import datetime
import os
import xarray as xr
import numpy as np
import zarr
from sqlalchemy import MetaData,text
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import JSONB,TIMESTAMP
from datetime import datetime
import pandas as pd

# from tqdm import tqdm
# from tqdm.dask import TqdmCallback
schema="prec_evap"

@as_declarative(metadata=MetaData(schema=schema))
class GleamTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id=Column(Integer,primary_key=True)
    lastupdate=Column(TIMESTAMP)
    var=Column(String)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    uri=Column(String)


class Gleam42aDailyBase(DataSet):
    schema=schema
    zarrfilebase="GLEAM_42a.zarr"
    var='E'

    def pull(self):
        auth=self.conf.authCred("gleam42",qryfields=["user","passw","url"])
        # note url should be of the form  sftp://server:port
        yrs=np.arange(1980,2024)        
        crwl=crawler(url=auth.url+"/data/v4.2a/daily",auth=auth)
        for yr in yrs:
            downdir=os.path.join(self.cacheDir(),str(yr))
            for uri in crwl.uris(subdirs=str(yr)):
                if uri.rpath.split("_")[0] == self.var:
                    uri.download(downdir,check=True)
    
        #convert to zarr storage
        self.convert2zarr()

        #if successfull the cache directory can be deleted

    def convert2zarr(self):
        slurplog.info(f"Converting GLEAM {self.var} to zarr store {self.zarrfile}")
        #open all datasets together
        gleam_files=sorted([f for f in Path(self.cacheDir()).rglob(f'*{self.var}_*.nc')])
        ds=xr.open_mfdataset(gleam_files,chunks='auto').chunk(dict(time=82,lon=800,lat=400))
        #save to zarr format
        compressor = zarr.Blosc(cname='lz4hc',shuffle=zarr.Blosc.SHUFFLE,clevel=9)
        encoding={self.var:dict(compressor=compressor)}
        if os.path.exists(self.zarrfile):
            mode='a'
        else:
            mode='w'
        ds.to_zarr(self.zarrfile, mode='w',encoding=encoding,compute=True)
    
    @property
    def zarrfile(self):
        return os.path.join(self.dataDir(),self.zarrfilebase)
    
    def register(self):
        #we're not actually going to read the whole thing
        ds=xr.open_dataset(self.zarrfile,chunks='auto')
        self.truncateTable()
        entry=dict(lastupdate=datetime.now(),var=self.var,tstart=pd.to_datetime(ds.time.data[0]),tend=pd.to_datetime(ds.time.data[-1]),uri=self.conf.generalize_path(self.zarrfile))
        self.addEntry(entry)
        self.updateInvent()

def getGleamDsets(conf):
    gleamclss=[]
    for var in ["E"]:
        clsName=f"Gleam42a_{var}"
        gtable=type(clsName+'Table',(GleamTBase,),{})
        gleamclss.append(type(clsName,(Gleam42aDailyBase,),{"var":var,"table":gtable}))
    return gleamclss
