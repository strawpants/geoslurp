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
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018
import copy
from datetime import datetime
import sys
from netCDF4 import Dataset as ncDset
import numpy as np
from geoslurp.datapull.uri import UriFile
from geoslurp.config.slurplogger import slurplogger
class BtdBox():
    """Class which holds a geographical bounding box, a vertical depth range and a datetime range,
    closely linked to the netcdffile operations below"""
    s=-90.0
    n=90.0
    w=0.0
    e=360.0
    ts=datetime.min
    te=datetime.max
    zmin=sys.float_info.min
    zmax=sys.float_info.max

    def __init__(self,s=None,n=None,w=None,e=None,ts=None,te=None,zmin=None,zmax=None):
        if s:
            self.s=s
        if n:
            self.n=n
        if w:
            self.w=w

        if e:
            self.e=e

        if ts:
            self.ts=ts
        if te:
            self.te=te
        if zmin:
            self.zmin=zmin

        if zmax:
            self.zmax=zmax

        self.check()

    def toGreenwhich(self):
        """returns an instance with the longitude coordinates to span -180 .. 180"""
        if self.e > 180:
            self.e-=360
        if self.w > 180:
            self.w-=360
        self.check()

    def to0_360(self):
        """Change the longitude coordinates to span 0 .. 360"""
        if self.e < 0:
            self.e+=360
        if self.w < 0:
            self.w+=360
        self.check()

    def check(self):
        """Check if the ranges are valid """
        if not (self.s < self.n and self.w < self.e and self.ts < self.te and self.zmin < self.zmax):
            raise ValueError("invalid Bounding box, time range or , z-range")

        if ( self.w < 0 and self.e > 180):
            raise ValueError("Inconsistent longitudes, mixing 0..360 with -180..180 bounds")

    def lonSplit(self,lon=0):
        """returns 2 bounding boxes from the current one split at a longitude
        """
        if lon < self.w or self.e < lon:
            raise RuntimeError("Splitting longitude not within the bounding box")
        left=copy.deepcopy(self)
        left.e=lon
        right=copy.deepcopy(self)
        right.w=lon
        return left,right

    def timeSplit(self,t=None):
        """return 2 bounding boxes split up at a certain time point"""
        if not t:
            t=self.ts+(self.te-self.ts)/2
        if t < self.ts or self.te < t:
            raise RuntimeError("Splitting Time dimension not within timerange")
        before=copy.deepcopy(self)
        before.te=t
        after=copy.deepcopy(self)
        after.ts=t
        return before,after

    def isGMTCentered(self):
        if ( self.w < 0 ) or (self.e < 0):
            return True
        else:
            return False

    def crop(self,btdbox):
        """Crops the bounding box based on the limits available in an other"""

        if self.s < btdbox.s:
            self.s=btdbox.s

        if btdbox.n < self.n:
            self.n=btdbox.n

        if self.w < btdbox.w:
            self.w=btdbox.w

        if btdbox.e < self.e:
            self.e=btdbox.e

        if self.zmin < btdbox.zmin:
            self.zmin=btdbox.zmin

        if btdbox.zmax < self.zmax:
            self.zmin=btdbox.zmax

        if self.ts < btdbox.ts:
            self.ts=btdbox.ts

        if btdbox.te < self.te:
            self.te=btdbox.te



def nccopyAtt(ncin,ncout,excl=[]):
    """Function to copy attributes from an open netcdf file to another"""
    for attnm in ncin.ncattrs():
        if attnm in excl:
            continue
        ncout.setncattr(attnm,ncin.getncattr(attnm))

def ncFileSwapCentralMeridian(ncinout,greenwhich=True):
    """swap the central meridian of a netcdf file in place"""
    pass

def stackNcFiles(ncout,ncA,ncB,dimension):
    """Append netcdf file B after file A along the dimension specified"""
    slurplogger().info("Patching files %s %s",ncA,ncB)
    #open the three netcdf files
    outid=ncDset(ncout,'w')
    aid=ncDset(ncA,'r')
    bid=ncDset(ncB,'r')

    # #copy global attributes
    nccopyAtt(aid,outid)

    # dimension to be excluded from dimension copy
    dexcl=[dimension]

    # make a list of variables which need to be appended  and cannot be copied straight away
    vapp=[ var for var in aid.variables.keys() if dimension in aid.variables[var].dimensions ]

    #copy dimensions (excluding the specified one)
    for nm,dim in aid.dimensions.items():
        if nm in dexcl:
            continue
        if dim.isunlimited():
            outid.createDimension(nm,None)
        else:
            outid.createDimension(nm,len(dim))

    # copy all variables  and attributes which don't require appending
    for nm, var in aid.variables.items():
        if nm in vapp:
            continue
        outid.createVariable(nm, var.datatype, var.dimensions)
        outid[nm][:] = aid[nm][:]
        nccopyAtt(aid[nm],outid[nm],['_FillValue'])

    #create new dimension
    outid.createDimension(dimension,aid.dimensions[dimension].size+bid.dimensions[dimension].size)

    #create new appended variables
    for var in vapp:
        outid.createVariable(var,aid[var].datatype,bid[var].dimensions)
        nccopyAtt(aid[var],outid[var],['_FillValue'])
        dimax=aid[var].dimensions.index(dimension)
        outid[var][:]=np.concatenate((aid[var][:],bid[var][:]),axis=dimax)

    outid.setncattr('History',outid.getncattr('History')+'\n Modified at %s by Geoslurp: Merge two netcdf files along dimension %s'%(datetime.now(),dimension))
    return UriFile(ncout),True
