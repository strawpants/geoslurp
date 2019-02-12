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

from geoslurp.datapull import CrawlerBase
from geoslurp.datapull import UriBase,UriFile
from geoslurp.config.slurplogger import slurplogger
from dateutil.parser import parse as isoParser
import os
from motu_utils.motu_api import execute_request
from datetime import datetime
from lxml import etree as XMLTree
from collections import namedtuple
from netCDF4 import num2date
import copy

# holds bounding box in geographical coordinates, a start/end time, and a depth range
Btdbox = namedtuple('Bbox', ['w', 'e', 'n', 's', 'ts', 'te', 'zmin', 'zmax'])

#set default arguments to Nones
Btdbox.__new__.__defaults__ = (None,) * len(Btdbox._fields)

class BtdBox():
    """Class which holds a geographical bounding box, a vertical depth range and a datetime range"""
    s=None
    n=None
    w=None
    e=None
    ts=None
    te=None
    zmin=None
    zmax=None
    def __init__(self,s=None,n=None,w=None,e=None,ts=None,te=None,zmin=None,zmax=None):

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
        out=copy.deepcopy(self)
        if out.e > 180:
            out.e-=360
        if out.w > 180:
            out.w-=360
        out.check()
        return out

    def to0_360(self):
        """Change the longitude coordinates to span 0 .. 360"""
        out=copy.deepcopy(self)
        if out.e < 0:
            out.e+=360
        if out.w < 0:
            out.w+=360
        out

    def check(self):
        """Check if the ranges are valid """
        if not (self.s < self.n and self.w < self.e and self.ts < self.te and self.zmin < self.zmax):
            raise RuntimeError("invalid Bounding box, time range or , z-range")

    def lonSplit(self,lon):
        """returns 2 bounding boxes from the current one split at a longitude
        """
        if lon < self.w or self.e < lon:
            raise RuntimeError("Splitting longitude not within the bounding box")
        left=copy.deepcopy(self)
        left.e=lon
        right=copy.deepcopy(self)
        right.w=lon
        return left,right

class MotuOpts():
    """A class which mimics the options from argparse as used by the motuclient command line program"""
    #list all configurable options here
    auth_mode='cas'
    user=None
    pwd=None
    motu=None
    product_id=None
    service_id=None
    latitude_max=None
    latitude_min=None
    longitude_min=None
    longitude_max=None
    depth_max=None
    depth_min=None
    variable=None
    out_dir='.'
    cache='.'
    out_name='dataset'
    describe=False
    extraction_geographic=True
    extraction_vertical=False
    proxy_server=None
    outputWritten=None
    console_mode=False
    socket_timeout=515
    sync=False
    block_size=12001
    size=False
    date_min=datetime.min.strftime('%Y-%m-%d %H:%M:%S')
    date_max=datetime.max.strftime('%Y-%m-%d %H:%M:%S')
    user_agent='motu-api-client'
    def __init__(self, moturoot,service,product,auth,btdbox,fout,cache,variables=None):
        """Sets options"""
        self.motu=moturoot
        self.service_id=service
        self.product_id=product
        self.user=auth.user
        self.pwd=auth.passw
        self.latitude_min=btdbox.s
        self.latitude_max=btdbox.n
        self.longitude_min=btdbox.w
        self.longitude_max=btdbox.e
        if btdbox.ts:
            self.date_min=btdbox.ts
        if btdbox.te:
            self.date_max=btdbox.te

        self.out_dir=os.path.dirname(fout)
        self.cache=cache
        self.out_name=os.path.basename(fout)
        self.variable=variables



class Uri(UriBase):
    kbsize=0
    maxkbsize=0
    info=False
    maxbtdbox=Btdbox()
    def __init__(self,Mopts):
        self.opts=Mopts
        url=os.path.join(Mopts.out_dir,Mopts.out_name)
        super().__init__(url)

    def requestInfo(self):
        """Request info (modification time, size, datacoverage) on this specific query from the server"""
        self.opts.describe=True
        oldd=self.opts.out_dir
        oldnm=self.opts.out_name
        self.opts.out_dir=self.opts.cache
        self.opts.out_name=self.opts.out_name.replace('.nc','_descr.nc')
        # try:
        #     execute_request(self.opts)
        # except Exception as e:
        #     slurplogger().error("failed to request info on query")
        #     raise(e)

        self.opts.describe=False
        self.opts.out_dir=oldd

        #read and parse xml
        xml=XMLTree.parse(os.path.join(self.opts.cache,self.opts.out_name.replace('.nc','.xml')))
        trange=xml.find('timeCoverage')
        self.lastmod=isoParser(trange.attrib['end']).replace(tzinfo=None)

        #also retrieve datacoverage
        covdict={}
        for axis in xml.iterfind('dataGeospatialCoverage/axis'):
            if axis.attrib['axisType'] == 'Lat':
                covdict['s']=float(axis.attrib['lower'])
                covdict['n']=float(axis.attrib['upper'])
            if axis.attrib['axisType'] == 'Lon':
                covdict['w']=float(axis.attrib['lower'])
                covdict['e']=float(axis.attrib['upper'])
            if axis.attrib['axisType'] == 'Time':
                covdict['ts']=num2date(float(axis.attrib['lower']),axis.attrib['units'])
                covdict['te']=num2date(float(axis.attrib['upper']),axis.attrib['units'])

        self.maxbtdbox=Btdbox(**covdict)

        #hack (change outname back to nc suffix)
        self.opts.out_name=oldnm

        self.info=True


    def updateModTime(self):
        """Requests data description from the motu service"""
        if not self.info:
            self.requestInfo()

    def updateSize(self):
        """Request information about the size of the query"""
        self.opts.size=True
        oldd=self.opts.out_dir
        self.opts.out_dir=self.opts.cache
        try:
            execute_request(self.opts)
        except Exception as e:
            slurplogger().error("failed to request size: %s",e)
            raise(e)

        self.opts.size=False
        self.opts.out_dir=oldd

        xml=XMLTree.parse(os.path.join(self.opts.cache,self.opts.out_name))
        self.kbsize=float(xml.getroot().attrib['size'])
        self.maxkbsize=float(xml.getroot().attrib['maxAllowedSize'])
        self.opts.out_name=self.opts.out_name.replace('.xml','.nc')


    def download(self,direc,check=False,gzip=False,outfile=None):
        #check whether the file exists and retrive thelast update date


        if outfile:
            self.opts.out_name=outfile

        self.opts.out_dir=direc

        fout=os.path.join(direc, self.opts.out_name)

        uri=UriFile(fout)
        if check and os.path.exists(fout):
            self.updateModTime()

            if self.lastmod <= uri.lastmod:
                slurplogger().info("No need to download file %s"%(fout))
                return uri,False



        slurplogger().info("Downloading %s"%(fout))
        try:
            execute_request(self.opts)
        except Exception as e:
            slurplogger().error("failed to download file %s",e)
            raise(e)
        return uri,True


class MotuComposite():
    """Class which splits up a larger motu request in binary children """
    A=None
    B=None
    def __init__(self,mopts):
        self.mopts=mopts
        # # request information on this request
        # self.muri=Uri(mopts)
        # #request datacoverage and update modification time
        # self.muri.requestInfo()
        # #check whether the central meridian of the requested and provided data agree
        # # if self.mopts.latitude_min < 0 && self.muri.maxkbsize > 0:

    def download(self):
        """Download all tiles"""
        pass

    def patch(self):
        """patch all tiles into a complete dataset"""
        pass

    def timeSplit(self):
        """splits the motu request at the half the time, until the size is small enough """
        pass

    def greenwhichSplit(self):
        """Splits the motu request at the 0 meridian"""
        muri=Uri(self.mopts)
        muri.requestInfo()

        pass

