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

envelopType = Geography(geometry_type="POLYGON", srid='4326')

class CDSBase(DataSet):
    """Provides a Base class from which subclasses can inherit to download CDS hosted data"""
    scheme="cds" 
    # The following need to be set in hte derived class
    resource=None
    productType=None
    variables=[]
    #may be overruled in derived classes
    columns=[Column('id',Integer,primary_key=True),Column("name",String,unique=True),
            Column("lastupdate",DateTime),Column("tstart",DateTime),Column("tend",DateTime),
            Column("uri", String),Column("data",JSON),Column("geom",envelopType)]
    
    #entries of the form {name:jsondict,..}
    reqdicts={}
    oformat='netcdf'
    description="CDS subset downloaded from cds.climate.copernicus.eu"
    res=0.0 # resolution of the pixels in the grid (used for adding a margin around the downloaded area when not 0)
    def __init__(self,dbconn):
        super().__init__(dbconn)
        if not "cds_jobs" in self._dbinvent.data:
            self._dbinvent.data["cds_jobs"]={}
    
        if self.oformat == 'netcdf':
            self.app=".nc"
        elif self.oformat == 'grib':
            self.app=".grb"
        elif self.oformat =='tgz':
            self.app=".tgz"

    def metaExtractor(self,uri):
        """implement this function in derived class"""
        raise NotImplementedError("MetaExtractor(self,uri) not implemented")
        return {}

    def pull(self,maxreq=100):
        dout=self.dataDir()
        
        cdsQueue=Cds(self.resource,self._dbinvent.data["cds_jobs"])
        #add requests to the CDS queue
        #Not it is expected that the derived class adds these requestdictionaries in one way or the other (default will be empty)
        
        #only submit maxreq at once
        nreq=0
        for name,reqdict in self.reqdicts.items():

            fout=os.path.join(dout,self.resource+"_"+name+self.app)
            cdsQueue.queueRequest(fout,reqdict)
            nreq+=1
            if nreq > maxreq:
                #do an intermediate download before submitting more requests
                #Sync the possibly updated queueinfo to the database
                self._dbinvent.data["cds_jobs"]=cdsQueue.jobqueue
                self._ses.commit()
        
                #wait for tasks to finish and download results to files
                cdsQueue.downloadQueue()
                cdsQueue.clearRequests()
                nreq=0

        #download outstanding jobs
        self._dbinvent.data["cds_jobs"]=cdsQueue.jobqueue
        self._ses.commit()

        #wait for tasks to finish and download results to files
        cdsQueue.downloadQueue()

        cdsQueue.clearRequests()

    def register(self):
        if not self.table:
            #create a new table on the fly
            self.createTable(self.columns)
        
        #create a list of files which need to be (re)registered
        newfiles=self.retainnewUris([UriFile(file) for file in findFiles(self.dataDir(),f".*\{self.app}$")])
        for uri in newfiles:
            meta=self.metaExtractor(uri)
            if not meta:
                #don't register empty entries
                continue
            slurplogger().info(f"Adding metadata from {uri.url}")
            self.addEntry(meta)
        self._dbinvent.data["Description"]=self.description
        self.updateInvent()

    def getDefaultDict(self,geomshape=None):
        #return a default cdsapi dictionary with common parameters
        
        reqdict={
                'format': self.oformat,
                'variable': self.variables,
                }
        
        if self.productType:
            reqdict["product_type"]=self.productType

        if geomshape:
            bb=geomshape.envelope.exterior.xy
            hres=self.res/2
            if np.min(bb[0]) > 180 or np.min(bb[0]) < 0:
                breakpoint()
            reqdict["area"]=[np.max(bb[1])+hres,np.min(bb[0])-hres,np.min(bb[1])-hres,np.max(bb[0])+hres]
        return reqdict


