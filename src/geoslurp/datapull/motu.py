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
from geoslurp.tools.Bounds import BtdBox
from geoslurp.tools.netcdftools import stackNcFiles
from geoslurp.config.slurplogger import slurplogger
from dateutil.parser import parse as isoParser
import os
from motu_utils.motu_api import execute_request
from datetime import datetime
from lxml import etree as XMLTree
from collections import namedtuple
from netCDF4 import num2date
import copy


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
    out_name='dataset.nc'
    describe=False
    extraction_geographic=True
    extraction_vertical=False
    proxy_server=None
    outputWritten="netcdf"
    console_mode=False
    socket_timeout=515
    sync=False
    block_size=12001
    size=False
    date_min=datetime.min.strftime('%Y-%m-%d %H:%M:%S')
    date_max=datetime.max.strftime('%Y-%m-%d %H:%M:%S')
    user_agent='motu-api-client'
    #custom variables
    btdbox=BtdBox()
    def __init__(self, moturoot,service,product,auth,btdbox,fout,cache,variables=None):
        """Sets options"""
        self.motu=moturoot
        self.service_id=service
        self.product_id=product
        self.user=auth.user
        self.pwd=auth.passw
        self.syncbtdbox(btdbox)
        self.cache=cache
        self.variable=variables
        self.syncfilename(fout)

    def syncbtdbox(self,bbox=None):
        """Sets the internal btdbox and synchronize the corresponding motu variables"""
        if bbox:
            self.btdbox=bbox

        self.latitude_min=self.btdbox.s
        self.latitude_max=self.btdbox.n
        self.longitude_min=self.btdbox.w
        self.longitude_max=self.btdbox.e
        if self.btdbox.ts:
            self.date_min=self.btdbox.ts.strftime('%Y-%m-%d %H:%M:%S')
        if self.btdbox.te:
            self.date_max=self.btdbox.te.strftime('%Y-%m-%d %H:%M:%S')

    def syncfilename(self,fout):
        self.out_dir=os.path.dirname(fout)
        self.out_name=os.path.basename(fout)

    def fullname(self):
        return os.path.join(self.out_dir,self.out_name)


class Uri(UriBase):
    kbsize=0
    maxkbsize=0
    info=False
    maxbtdbox=BtdBox()
    def __init__(self,Mopts):
        self.opts=Mopts
        url=os.path.join(Mopts.out_dir,Mopts.out_name)
        super().__init__(url)

    def requestInfo(self):
        """Request info (modification time, size, datacoverage) on this specific query from the server"""
        if self.info:
            #quick return when already done
            return

        self.opts.describe=True
        oldd=self.opts.out_dir
        oldnm=self.opts.out_name
        self.opts.out_dir=self.opts.cache
        self.opts.out_name=self.opts.out_name.replace('.nc','_descr.xml')
        # import pdb;pdb.set_trace()
        try:
            execute_request(self.opts)
        except Exception as e:
            slurplogger().error("failed to request info on query")
            raise(e)

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

        self.maxbtdbox=BtdBox(**covdict)

        #Crop/Synchronize the requested bounding box with that what is available
        self.opts.btdbox.crop(self.maxbtdbox)
        self.opts.syncbtdbox()



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
        # self.opts.out_name=self.opts.out_name.replace('.nc','.xml')

        self.opts.size=False
        self.opts.out_dir=oldd

        xml=XMLTree.parse(os.path.join(self.opts.cache,self.opts.out_name))
        self.kbsize=float(xml.getroot().attrib['size'])
        self.maxkbsize=float(xml.getroot().attrib['maxAllowedSize'])
        self.opts.out_name=self.opts.out_name.replace('.xml','.nc')

        return self.kbsize,self.maxkbsize

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


class MotuRecursive():
    """Class which recursively downloads netcdf files within the 1GB limit using motu and patches them together"""
    keepfiles=False
    def __init__(self,mopts,keepfiles=False):
        self.mopts=mopts
        self.keepfiles=keepfiles

    def download(self):
        """Download file"""
        muri=Uri(self.mopts)

        #check if download is needed
        muri.requestInfo()
        uristacked=UriFile(self.mopts.fullname())
        if uristacked.lastmod:
            if muri.lastmod <= uristacked.lastmod:
                slurplogger().info("Already downloaded %s"%(uristacked.url))
                #quick return when there is no need to merge/download
                return uristacked,False

        #check if download is allowed
        kb,maxkb=muri.updateSize()
        if kb > maxkb:
            #split up request and try again

            #create 2 bounding boxes split on time
            Abbox,Bbbox=muri.opts.btdbox.timeSplit()

            AmotuRec=MotuRecursive(copy.deepcopy(self.mopts))
            AmotuRec.mopts.syncbtdbox(Abbox)
            AmotuRec.mopts.out_name=self.mopts.out_name.replace('.nc','_A.nc')
            AmotuRec.mopts.out_dir=AmotuRec.mopts.cache

            BmotuRec=MotuRecursive(copy.deepcopy(self.mopts))
            BmotuRec.mopts.syncbtdbox(Bbbox)
            BmotuRec.mopts.out_name=self.mopts.out_name.replace('.nc','_B.nc')
            BmotuRec.mopts.out_dir=BmotuRec.mopts.cache

            Auri,Aupd=AmotuRec.download()
            Buri,Bupd=BmotuRec.download()

            #possible improvement here split a dataset at an unlimited dimensions and append the second one to the first one
            #patch files together (if updated)
            if Aupd or Bupd or not os.path.exists(self.mopts.fullname()):
                uristacked,upd=stackNcFiles(self.mopts.fullname(),Auri.url,Buri.url,'time')
                if not self.keepfiles:
                    #remove the partial files
                    os.remove(AmotuRec.mopts.fullname())
                    os.remove(BmotuRec.mopts.fullname())
            else:
                uristacked=UriFile(self.mopts.fullname())
                upd=False
            return uristacked, True
        else:
            return muri.download(self.mopts.out_dir,check=True)

