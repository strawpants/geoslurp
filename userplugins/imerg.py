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

from geoslurp.dataset.RasterBase import RasterBase
from geoslurp.datapull.thredds import Crawler as ThreddsCrawler,ThreddsFilter
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.config.slurplogger import slurplog
from datetime import datetime
from sqlalchemy import Column,String
import os
import xarray as xr
from geoslurp.datapull.http import Uri as http
from geoslurp.tools.Bounds import BtdBox
from dateutil.parser import parse
from cftime import DatetimeJulian
import numpy as np

class imerg_monthly(RasterBase):
    outofdb=False
    scheme="prec_evap"
    srid=4326
    preview={"bandnr":1,"bandname":"precipitation"}
    swapxy=True
    rastregex="\.nc$"
    auxcolumns=[Column("name",String)]
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.cookiefile=os.path.join(self.cacheDir(),"cookies.txt")
        self.dscoords=None
        self.pullqueue={}
        self.rooturl="https://gpm1.gesdisc.eosdis.nasa.gov/opendap/ncml/aggregation/GPM_3IMERGM.06/GPM_3IMERGM.06_Aggregation.ncml.nc4"

    def pull(self, name=None, wsne=None,tstart=datetime.min,tend=datetime.max):
        """Pulls a subset of the imerg gridded dataset as netcdf from an motu enabled server
        This routine calls the internal routines of the motuclient python client
        :param name: Name of the  output datatset (file will be named 'name.nc')
        :param wsne: bounding box of the section of interest as [West,South,North,East]
        :param tstart: start date (as yyyy-mm-dd) for the extraction
        :param tend: end date (as yyyy-mm-dd) for the extraction
        """
        
        if name is not None:
            self.queueDownloadjob(name,wsne,tstart,tend)

        auth=self.conf.authCred("podaac")
        ddir=self.dataDir()
        for ky,val in self.pullqueue.items():
            url=self.rooturl+val
            fout=os.path.join(ddir,ky+".nc")
            http(url,cookiefile=self.cookiefile,auth=auth,lastmod=datetime(2022,7,1)).download(ddir,outfile=fout,check=True)
        
        
    def loadCoordinates(self):
        """Download the time, lon and lat coordinates and load these coordinates from the netdf file"""
        if self.dscoords is not None:
            return

        auth=self.conf.authCred("podaac")
        cache=self.cacheDir()
        coordfile=os.path.join(cache,"Coordinates.nc")
        opendapcoordinates=self.rooturl+"?time[0:1:255],lat[0:1:1799],lon[0:1:3599]"
        
        http(opendapcoordinates,cookiefile=self.cookiefile,auth=auth,lastmod=datetime(2022,7,1)).download(cache,outfile=os.path.basename(coordfile),check=True)

        self.dscoords=xr.open_dataset(coordfile,decode_times=True)

    def queueDownloadjob(self,name,wsne,tstart=datetime.min,tend=datetime.max):
        if type(tstart)  == str:
            tstart=parse(tstart)

        tstart=DatetimeJulian(tstart.year,tstart.month,tstart.day)
        if type(tend)  == str:
            tend=parse(tend)
        tend=DatetimeJulian(tend.year,tend.month,tend.day)
        
        self.loadCoordinates()
        bbox=BtdBox(w=wsne[0],n=wsne[2],s=wsne[1],e=wsne[3],ts=tstart,te=tend)
        
        #retrieve the appropriate index range
        halfres=0.1/2
        trange=self.getRange(bbox.ts,bbox.te,self.dscoords.time)
        latrange=self.getRange(bbox.s-halfres,bbox.n+halfres,self.dscoords.lat)
        lonrange=self.getRange(bbox.w-halfres,bbox.e+halfres,self.dscoords.lon)
        if trange is None or latrange is None or lonrange is None:
            slurplog.warning("refusing to queue empty dataset, skipping")
            return
        qrystr=f"?time{trange},lat{latrange},lon{lonrange},precipitation{trange}{lonrange}{latrange}"
        self.pullqueue[name]=qrystr
    
    @staticmethod
    def getRange(start,end,within):

        idx=np.where((within >= start) & (within <= end))
        if len(idx[0]) < 1:
            return None
        return f"[{idx[0][0]}:1:{idx[0][-1]}]"
    
    def rastExtract(self,uri):
       meta=super().rastExtract(uri)
       meta["name"]=os.path.basename(uri.url)[0:-3]
       return meta

geoslurpCatalogue.addDataset(imerg_monthly)
