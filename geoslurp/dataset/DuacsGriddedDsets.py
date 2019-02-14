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
from geoslurp.meta.netcdftools import BtdBox

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
import numpy as np

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
    updated=[]
    def __init__(self,scheme):
        super().__init__(scheme)
        DuacsTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self, name=None, west=None,east=None,north=None,south=None,tstart=None,tend=None):
        """Pulls a subset of a gridded dataset as netcdf from the cmems copernicus server
        This routine calls the internal routines of the motuclient python client
        :param name: Name of the  output datatset (file will be 'named name.nc')
        :param west: most western longitude of the bounding box
        :param east: most eastern longitude of the bounding box
        :param south: most southern latitude of the bounding box
        :param north: most northern longitude of the bounding box
        :param tstart: start date (as yyyy-mm-dd) for the extraction
        :param tend: end date (as yyyy-mm-dd) for the extraction
        bbox.n,bbox.s)
        """

        if not name:
            raise RuntimeError("A name must be supplied to Duacs.pull !!")


        if None in [west,east,north,south]:
            raise RuntimeError("Please supply a name and a geographical bounding box")

        try:
            bbox=BtdBox(w=west,e=east,n=north,s=south,ts=tstart,te=tend)
        except:
            raise RuntimeError("Invalid bounding box provided to Duacs pull")


        cred=self.scheme.conf.authCred("cmems")
        downloaddir=self.dataDir()


        if bbox.isGMTCentered():
            # we need 2 downloads and a merging of the grids !
            # split the bounding box in two
            bboxleft,bboxright=bbox.lonSplit(0.0)
            bboxleft.to0_360()
            bboxright.to0_360()
        else:
            #we can handle this by a single recursive motu instance
            ncout=os.path.join(downloaddir,name+".nc")

            mOpts=MotuOpts(moturoot="http://my.cmems-du.eu/motu-web/Motu",service='SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS',
                       product="dataset-duacs-rep-global-merged-allsat-phy-l4",btdbox=bbox,fout=ncout,cache=self.cacheDir(),variables=['sla'],auth=cred)
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
            files=[UriFile(file) for file in findFiles(self.dataDir(),'.*nc')]

        #loop over files
        for uri in files:

            urilike=uri.url

            if not self.uriNeedsUpdate(urilike,uri.lastmod):
                continue
            meta=duacsMetaExtractor(uri)
            self.addEntry(meta)

        self._inventData["lastupdate"]=datetime.now().isoformat()
        self.updateInvent()

        self.ses.commit()


def getDuacsDict():
    """return a dict"""
    return {"Duacs":Duacs}