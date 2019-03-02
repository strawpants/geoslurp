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
from geoslurp.meta.netcdftools import BtdBox,ncSwapLongitude,stackNcFiles
from geoslurp.config.register import geoslurpregistry

import os
from geoalchemy2.types import Geography
from sqlalchemy import Column,Integer,String, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from netCDF4 import Dataset as ncDset
from datetime import datetime,timedelta
from geoslurp.datapull import findFiles,UriFile
from geoslurp.config.slurplogger import slurplogger
import copy

DuacsTBase=declarative_base(metadata=MetaData(schema='altim'))

geoBbox = Geography(geometry_type="POLYGONZ", srid='4326', spatial_index=True,dimension=3)

class DuacsTable(DuacsTBase):
    """Defines the Duacs gridded altimetry PostgreSQL table"""
    __tablename__='duacs'
    id=Column(Integer,primary_key=True)
    name=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    uri=Column(String)
    geom=Column(geoBbox)
    data=Column(JSONB)


def duacsMetaExtractor(uri):
    """Extracts data from a netcdf file"""

    meta={}
    url=uri.url
    ncalt=ncDset(url)

    slurplogger().info("Extracting meta info from: %s"%(url))

    # Get reference time
    t0=datetime(1950,1,1)
    data={}
    timevec=[t0+timedelta(days=int(x),seconds=86400*divmod(x,int(x))[1]) for x in ncalt['time'][:] ]

    data["time"]=[t.isoformat() for t in timevec]

    minlat=min(ncalt['latitude'][:])
    maxlat=max(ncalt["latitude"][:])
    minlon=min(ncalt["longitude"][:])
    maxlon=max(ncalt["longitude"][:])

    polystr='SRID=4326;POLYGONZ(('
    sep=''
    for x,y in [(minlon,minlat),(minlon,maxlat),(maxlon,maxlat),(maxlon,minlat), (minlon,minlat)]:
        polystr+="%s%f %f 0"%(sep,x,y)
        sep=','
    polystr+='))'

    return {"lastupdate":uri.lastmod, "name":os.path.basename(url).split('.')[0],
                 "tstart":min(timevec), "tend":max(timevec), "uri":url,
                 "geom":polystr,"data":data}



class Duacs(DataSet):
    """Downloads subsets of the ducacs gridded multimission altimeter datasets for given regions"""
    table=DuacsTable
    scheme='altim'
    updated=[]
    def __init__(self,dbconn):
        super().__init__(dbconn)
        DuacsTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self, name=None, wsne=None,tstart=None,tend=None):
        """Pulls a subset of a gridded dataset as netcdf from the cmems copernicus server
        This routine calls the internal routines of the motuclient python client
        :param name: Name of the  output datatset (file will be named 'name.nc')
        :param wsne: bounding box of the section of interest as [West,South,North,East]
        :param tstart: start date (as yyyy-mm-dd) for the extraction
        :param tend: end date (as yyyy-mm-dd) for the extraction
        """

        if not name:
            raise RuntimeError("A name must be supplied to Duacs.pull !!")


        if None in wsne:
            raise RuntimeError("Please supply a geographical bounding box")

        try:
            bbox=BtdBox(w=wsne[0],n=wsne[2],s=wsne[1],e=wsne[3],ts=tstart,te=tend)
        except:
            raise RuntimeError("Invalid bounding box provided to Duacs pull")


        cred=self.conf.authCred("cmems")
        ncout=os.path.join(self.dataDir(),name+".nc")

        mOpts=MotuOpts(moturoot="http://my.cmems-du.eu/motu-web/Motu",service='SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS',
                       product="dataset-duacs-rep-global-merged-allsat-phy-l4",btdbox=bbox,fout=ncout,cache=self.cacheDir(),variables=['sla'],auth=cred)

        if bbox.isGMTCentered():
            # we need 2 downloads and a merging of the grids !
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
                #change the longitude representation to -180..0
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


    def register(self):
        """Register regional grids in the database"""
        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in findFiles(self.dataDir(),'.*nc',self._dbinvent.lastupdate)]
        newfiles=self.retainnewUris(files)
        #loop over files
        for uri in newfiles:
            meta=duacsMetaExtractor(uri)
            self.addEntry(meta)

        self.updateInvent()


geoslurpregistry.registerDataset(Duacs)
