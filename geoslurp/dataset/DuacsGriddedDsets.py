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
from geoslurp.datapull.motu import MotuOpts
from geoslurp.datapull.motu import Btdbox
from geoslurp.datapull.motu import MotuComposite

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

def nccopyAtt(ncin,ncout,excl=[]):
    """Quick function to copy attributes from a netcdf entity to another"""
    for attnm in ncin.ncattrs():
        if attnm in excl:
            continue
        ncout.setncattr(attnm,ncin.getncattr(attnm))


def duacsMergeGrids(name,fromdir,todir):
    """Merges two grid files which are split on the 0 meridian and convert to -180,180 longitude domain"""
    #open the first grid and use this as a base
    left=os.path.join(fromdir,name+'_left.nc')
    right=os.path.join(fromdir,name+'_right.nc')
    out=os.path.join(todir,name+'.nc')

    ncleft=ncDset(left,'r')
    ncright=ncDset(right,'r')
    ncout=ncDset(out,'w')

    #copy relevant dimensions, attributes and variables from old to new grid
    nccopyAtt(ncleft,ncout)

    dexcl=['longitude']
    vexcl=['longitude','sla']

    #copy dimensions (excluding longitude)
    for nm,dim in ncleft.dimensions.items():
        if nm in dexcl:
            continue
        if dim.isunlimited():
            ncout.createDimension(nm,None)
        else:
            ncout.createDimension(nm,len(dim))

    # copy all file data for variables that are included in the toinclude list
    for nm, var in ncleft.variables.items():
        if nm in vexcl:
            continue

        ncout.createVariable(nm, var.datatype, var.dimensions)
        ncout[nm][:] = ncleft[nm][:]
        nccopyAtt(ncleft[nm],ncout[nm],['_FillValue'])

    #create new longitude dimension and variable

    #adapt longitude to new boundaries
    lonn = np.concatenate((ncleft['longitude'][:] - 360, ncright["longitude"][:]))
    nm='longitude'
    ncout.createDimension(nm,len(lonn))
    ncout.createVariable(nm,ncleft[nm].datatype,(nm))
    #copy/adapt attributes
    nccopyAtt(ncleft[nm],ncout[nm],['_FillValue'])
    ncout[nm].setncattr('valid_max',-180.0)
    ncout[nm].setncattr('valid_min',180.0)
    ncout[nm][:]=lonn

    #create new sla grid
    nm='sla'
    ncout.createVariable(nm,ncleft[nm].datatype,ncleft[nm].dimensions)
    ncout[nm].set_auto_maskandscale(True)
    nccopyAtt(ncleft[nm],ncout[nm],['_FillValue'])
    for i in range(ncout.dimensions['time'].size):
        ncout[nm][i,:,:]=np.concatenate((ncleft[nm][i,:,:], ncright[nm][i,:,:]),axis=1)

    #adapt some attributes
    ncout.setncattr('geospatial_lon_max',max(lonn))
    ncout.setncattr('geospatial_lon_min',min(lonn))



    ncout.setncattr('History',ncout.getncattr('History')+'\n Modified by Geoslurp: Merge two grids alongside 0-meridian')

    return UriFile(out),True

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

    def pull(self, name=None, west=None,east=None,north=None,south=None):
        """Pulls a subset of a gridded dataset as netcdf from the cmems copernicus server
        This routine calls the internal routines of the motuclient python client
        :param name: Name of the  output datatset (file will be 'named name.nc')
        :param west: most western longitude of the bounding box
        :param east: most eastern longitude of the bounding box
        :param south: most southern latitude of the bounding box
        :param north: most northern longitude of the bounding box
        bbox.n,bbox.s)
        """

        if not name:
            raise RuntimeError("A name must be supplied to Duacs.pull !!")

        #reserved for code which will check whether an entry allready exists in the database (will extract bounding box from that entry)


        if None in [west,east,north,south]:
            raise RuntimeError("Please supply a name and a geographical bounding box")

        # # convert longitude to 0-360 degree domain
        # if west <0:
        #     west+=360
        # if east < 0:
        #     east+=360


        ncout=name+".nc"
        cred=self.scheme.conf.authCred("cmems")
        downloaddir=self.dataDir()

        bbox=Btdbox(w=west, e=east, s=south, n=north)

        mOpts=MotuOpts(moturoot="http://my.cmems-du.eu/motu-web/Motu",service='SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS',
                       product="dataset-duacs-rep-global-merged-allsat-phy-l4",btdbox=bbox,fout=ncout,cache=self.cacheDir(),variables=['sla'],auth=cred)
        Mcomposite=MotuComposite(mOpts)

        #test whether the zero meridian is within the box (then the request must be split in 2 parts and the grids merged)
        bbox2=False
        if east < west:
            bbox=Btdbox(w=west, e=360, s=south, n=north)
            ncout=name+"_left.nc"
            bbox2=Btdbox(w=0, e=east, s=south, n=north)
            ncout2=name+"_right.nc"
            downloaddir=self.cacheDir()

        #construct an option object which is fed into the motuclient
        mOpts=MotuOpts(moturoot="http://my.cmems-du.eu/motu-web/Motu",service='SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS',
                         product="dataset-duacs-rep-global-merged-allsat-phy-l4",btdbox=bbox,fout=ncout,cache=self.cacheDir(),variables=['sla'],auth=cred)

        #create a MOTU
        uri=MotuUri(mOpts)
        uri.updateModTime()
        try:
            tmpuri,upd=uri.download(downloaddir,check=True)
        except:
            raise RuntimeError("Downloading of %s, failed (try again later?)"%(ncout))

        if bbox2:
            #also download the other part and merge the file
            mOpts=MotuOpts(moturoot="http://my.cmems-du.eu/motu-web/Motu",service='SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS',
                       product="dataset-duacs-rep-global-merged-allsat-phy-l4",bbox=bbox2,fout=ncout2,cache=self.cacheDir(),variables=['sla'],auth=cred)
            uri2=MotuUri(mOpts)
            try:
                tmpuri2,upd=uri2.download(downloaddir,check=True)
            except:
                raise RuntimeError("Downloading of %s, failed (try again later?)"%(ncout))

            tmpuri,upd=duacsMergeGrids(name,downloaddir,self.dataDir())


        if upd:
            self.updated.append(tmpuri)


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