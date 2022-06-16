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

from geoslurp.dataset import DataSet
from geoslurp.datapull.motu import Uri as MotuUri
from geoslurp.datapull.motu import MotuOpts, MotuRecursive
from geoslurp.tools.Bounds import BtdBox
from geoslurp.tools.netcdftools import ncSwapLongitude,stackNcFiles

import os
from geoalchemy2.types import Geography
from sqlalchemy import Column,Integer,String, Boolean,ARRAY
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from netCDF4 import Dataset as ncDset

from netCDF4 import num2date
from datetime import datetime,timedelta
from geoslurp.datapull import findFiles,UriFile
from geoslurp.config.slurplogger import slurplogger
from geoslurp.dataset.RasterBase import RasterBase
import copy
from dateutil.parser import parse

class MotuGridsBase(RasterBase):
    """Downloads and register subsets of gridded data with the motu client"""
    outofdb=False
    rastregex="\.nc$"
    auxcolumns=[Column("lastupdate",TIMESTAMP),Column("time",ARRAY(TIMESTAMP))]
    updated=[]
    moturoot=None
    motuservice=None
    motuproduct=None
    authalias=None
    #the variable of interest to retrieve
    variables=None
    def __init__(self,dbconn):
        super().__init__(dbconn)

    def pull(self, name=None, wsne=None,tstart=None,tend=None):
        """Pulls a subset of a gridded dataset as netcdf from an motu enabled server
        This routine calls the internal routines of the motuclient python client
        :param name: Name of the  output datatset (file will be named 'name.nc')
        :param wsne: bounding box of the section of interest as [West,South,North,East]
        :param tstart: start date (as yyyy-mm-dd) for the extraction
        :param tend: end date (as yyyy-mm-dd) for the extraction
        """
        
        if type(tstart)  == str:
            tstart=parse(tstart)

        if type(tend)  == str:
            tend=parse(tend)

        if not name:
            raise RuntimeError("A name must be supplied to MotuGridsBase.pull !!")


        if None in wsne:
            raise RuntimeError("Please supply a geographical bounding box")

        try:
            bbox=BtdBox(w=wsne[0],n=wsne[2],s=wsne[1],e=wsne[3],ts=tstart,te=tend)
        except:
            raise RuntimeError("Invalid bounding box provided to Duacs pull")


        cred=self.conf.authCred(self.authalias)
        ncout=os.path.join(self.dataDir(),name+".nc")

        mOpts=MotuOpts(moturoot=self.moturoot,service=self.motuservice,
                       product=self.motuproduct,btdbox=bbox,fout=ncout,cache=self.cacheDir(),variables=self.variables,auth=cred)

        if bbox.isGMTCentered():
            # we need 2 downloads to split the  and a merging of the grids !
            # split the bounding box in two
            bboxleft,bboxright=bbox.lonSplit(0.0)
            bboxleft.to0_360()
            bboxright.to0_360()

            ncoutleft=os.path.join(self.cacheDir(),name+"_left.nc")

            mOptsleft=copy.deepcopy(mOpts)
            mOptsleft.syncbtdbox(bboxleft)
            mOptsleft.syncfilename(ncoutleft)

            MotuRecleft=MotuRecursive(mOptsleft)
            urileft,updleft=MotuRecleft.download()

            ncoutright=os.path.join(self.cacheDir(),name+"_right.nc")
            mOptsright=copy.deepcopy(mOpts)
            mOptsright.syncbtdbox(bboxright)
            mOptsright.syncfilename(ncoutright)

            MotuRecright=MotuRecursive(mOptsright)
            uriright,updright=MotuRecright.download()

            stackNcFiles(ncout,urileft.url,uriright.url,'longitude')
            if updleft or updright:
                #change the longitude representation to -180..0 (without reshuffeling the data
                ncSwapLongitude(urileft.url)
                # patch files
                uri,upd=stackNcFiles(ncout,urileft.url,uriright.url,'longitude')
            else:
                upd=False
                uri=UriFile(ncout)
        else:
            #we can handle this by a single recursive motu instance

            MotuRec=MotuRecursive(mOpts)
            uri,upd=MotuRec.download()

        if upd:
            self.updated.append(uri)

    def rastExtract(self,uri):
        """extract raster and other meta info from the downloaded files"""
        meta=super().rastExtract(uri)
        meta["lastupdate"]=uri.lastmod
        ncid=ncDset(uri.url)
        time=ncid.variables["time"]
        # import pdb;pdb.set_trace()
        # t0=datetime(1950,1,1)
        meta["time"]=num2date(time[:],time.units,only_use_cftime_datetimes=False)
        # s=int(x),seconds=int(86400*divmod(x,int(x))[1])) for x in ncid['time'][:]]
        return meta

     
