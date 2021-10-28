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
from geoslurp.config.slurplogger import slurplogger
import cdsapi
import os
import numpy as np
from geoalchemy2 import Geography
from shapely.geometry import Polygon
from shapely.wkt import dumps as wktdumps
from netCDF4 import Dataset as ncDset
from netCDF4 import num2date
import time
from urllib.error import HTTPError

envelopType = Geography(geometry_type="POLYGON", srid='4326')

def ERA5MetaExtractor(ncuri):
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
    return {"name":name,"lastupdate":ncuri.lastmod,"tstart":tstart,"tend":tend,"uri":ncuri.url,"data":data,
            "geom":wktdumps(bbox)}

class ERA5Base(DataSet):
    """Provides a Base class from which subclasses can inherit to download a subset of the data per area"""
    scheme="atmo"
    dset='reanalysis-era5-pressure-levels-monthly-means'
    productType='monthly_averaged_reanalysis'
    yrstart=2000
    yrend=2000
    variables={}
    time="00:00"
    columns=[Column('id',Integer,primary_key=True),Column("name",String,unique=True),
            Column("lastupdate",DateTime),Column("tstart",DateTime),Column("tend",DateTime),
            Column("uri", String),Column("data",JSON),Column("geom",envelopType)]

    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.areas_nwse={}
        if not "cds_jobs" in self._dbinvent.data:
            self._dbinvent.data["cds_jobs"]={}

    def appendRequest(self,name,area):
        bb=area.envelope.exterior.xy
        
        self.areas_nwse[name]=[np.max(bb[1]),np.min(bb[0]),np.min(bb[1]),np.max(bb[0])]

    def pull(self):
        if not os.path.exists(os.path.join(os.path.expanduser("~"),".cdsapirc")):
            raise RuntimeError("Before using the cdsapi please visit https://cds.climate.copernicus.eu/api-how-to to obtain a token and setup your ~/.cdsapirc file")
        dout=self.dataDir()
        requests=[]
        #start a client (which allows queing jobs in the bacjground)
        client = cdsapi.Client(wait_until_complete=False)
        for name,area in self.areas_nwse.items():
            fout=os.path.join(dout,self.dset+"_"+name+".nc")
            if os.path.exists(fout):
                slurplogger().info(f"Already downloaded ERA5 data for area {name}")
                continue
            
            req_id=None
            #possibly get the request id from a previously queued job
            if fout in self._dbinvent.data["cds_jobs"]:
                req_id=self._dbinvent.data["cds_jobs"][fout]
            
            
            if req_id:
                #try to get an existing job
                slurplogger().info(f"Trying to retrieve previously queued job for {fout}")
                try:
                    req=cdsapi.api.Result(client,dict(request_id=req_id))
                    req.update()
                except: 
                    #Job cannot be found anymore
                    slurplogger().info(f"Job cannot be found for {fout}, retrying")
                    req_id=None
                    del self._dbinvent.data["cds_jobs"][fout]

            if not req_id:
                #start a new request
                slurplogger().info(f"Queuing new ERA5 request for {fout}")
                requestdict=self.getRequestDict(area)
                req=client.retrieve(self.dset,requestdict)
                req.update()
                req_id=req.reply["request_id"]
                #add an entry to the inventory
                self._dbinvent.data["cds_jobs"][fout]=req_id
            
            requests.append((req,fout,req.reply["state"]))
        #Sync the possibly updated queueinfo to the database
        self._ses.commit()
        
        #wait for tasks to finish and download results to files
        sleep=30
        nDownloaded=0
        nFailed=0
        while (nDownloaded+nFailed) < len(requests):
            # don't be too pushy and wait a while before checking
            time.sleep(sleep)
            for i,(req,fout,stateprev) in enumerate(requests):
                # import pdb;pdb.set_trace()
                if not fout:
                    #already downloaded the file in this queue
                    continue
                req.update()
                reply = req.reply
                req_id=reply["request_id"]
                state=reply['state']

                if state != stateprev:
                    slurplogger().info(f"Request ID: {req_id}, changed state to:{state}")
                    requests[i]=(req,fout,state)

                if state == "completed":
                    #download file
                    slurplogger().info(f"Downloading ERA5 request for {fout}")
                    req.download(fout)
                    #mark as done downloading (replace entry with None tuple)
                    requests[i]=(None,None,None)
                    nDownloaded+=1
                elif state in ("failed",):
                    nFailed+=1
                    slurplogger().error(f'Message: {reply["error"].get("message")}')
                    slurplogger().error(f'Reason: {reply["error"].get("reason")}')
                    for n in (
                        reply.get("error", {}).get("context", {}).get("traceback", "").split("\n")
                    ):
                        if n.strip() == "":
                            break
                        slurplogger().error("  %s", n)
                    raise Exception(
                        f'reply["error"].get("message")  reply["error"].get("reason")'
                    )
            
    def getRequestDict(self,area):
        """Builds a dictionary for the cdsapi
        :param area (shapely geometry) geometry which will be used to compute the bounding box to download data for"""
        reqdict={
                'format': 'netcdf',
                'product_type': self.productType,
                'variable': self.variables,
                'pressure_level':self.plevels,
                'year': [f"{yr}" for yr in range(self.yrstart,self.yrend+1)],
                'month':[f"{mn:02d}" for mn in range(1,13)],
                'time': self.time,
                'area': area}
        return reqdict

    def register(self):
        if not self.table:
            #create a new table on the fly
            self.createTable(self.columns)
        #create a list of files which need to be (re)registered
        newfiles=self.retainnewUris([UriFile(file) for file in findFiles(self.dataDir(),".*\.nc$")])
        for uri in newfiles:
            meta=ERA5MetaExtractor(uri)
            if not meta:
                #don't register empty entries
                continue
            self.addEntry(meta)
        self._dbinvent.data["Description"]="ERA5 subset downloaded from cds.climate.copernicus.eu"
        self.updateInvent()



