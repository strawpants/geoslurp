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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019
from datetime import datetime
import sys
from copy import deepcopy
from shapely.geometry import Polygon

class BtdBox():
    """Class which holds a geographical bounding box, a vertical depth range and a datetime range"""


    def __init__(self,s=-90.0,n=90.0,w=0.0,e=360.0,ts=datetime.min,te=datetime.max,zmin=sys.float_info.min,zmax=sys.float_info.max):
        self.s=s
        self.n=n
        self.w=w
        self.e=e
        self.ts=ts
        self.te=te
        self.zmin=zmin
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
        if self.e <= 0:
            self.e+=360
        if self.w < 0:
            self.w+=360
        self.check()

    def check(self):
        """Check if the ranges are valid """
        if not (self.s < self.n and self.w < self.e and self.ts <= self.te and self.zmin < self.zmax):
            raise ValueError("invalid Bounding box, time range or , z-range")

        if ( self.w < 0 and self.e > 180):
            raise ValueError("Inconsistent longitudes, mixing 0..360 with -180..180 bounds")

    def lonSplit(self,lon=0):
        """returns 2 bounding boxes from the current one split at a longitude
        """
        if lon < self.w or self.e < lon:
            raise RuntimeError("Splitting longitude not within the bounding box")
        left=deepcopy(self)
        left.e=lon
        right=deepcopy(self)
        right.w=lon
        return left,right

    def timeSplit(self,t=None):
        """return 2 bounding boxes split up at a certain time point"""
        if not t:
            t=self.ts+(self.te-self.ts)/2
        if t < self.ts or self.te < t:
            raise RuntimeError("Splitting Time dimension not within timerange")
        before=deepcopy(self)
        before.te=t
        after=deepcopy(self)
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

    def wkt(self):
        """Returns a WKT represetation of the geographical bounding box"""
        return "POLYGON(( %f %f, %f %f, %f %f, %f %f, %f %f ))"%(self.w,self.n,self.w,self.s,self.e,self.s,self.e,self.n,self.w,self.n)

    def poly(self):
        return Polygon.from_bounds(self.w, self.s, self.e, self.n)
    
    def corners(self):
        #(minx, maxx, miny, maxy) 
        return [self.w, self.e, self.s, self.n]
    
    def w(self):
        return self.w
    
    def e(self):
        return self.e
    
    def s(self):
        return self.s
    
    def n(self):
        return self.n
