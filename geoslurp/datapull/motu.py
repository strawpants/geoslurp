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
from dateutil.parser import parse as isoParser
import os
from motu_utils.motu_api import execute_request
from datetime import datetime
from lxml import etree as XMLTree
import logging

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
    date_max=None
    date_min=None
    variable=None
    out_dir='.'
    out_name='dataset'
    describe=False
    extraction_geographic=True
    extraction_vertical=False
    proxy_server=None
    outputWritten=None
    console_mode=False
    socket_timeout=515
    sync=True
    block_size=12001
    size=False
    date_min=datetime.min.strftime('%Y-%m-%d %H:%M:%S')
    date_max=datetime.max.strftime('%Y-%m-%d %H:%M:%S')
    def __init__(self, moturoot,service,product,auth,bbox,fout,variables=None):
        """Sets options"""
        self.motu=moturoot
        self.service_id=service
        self.product_id=product
        self.user=auth.user
        self.pwd=auth.passw
        self.latitude_min=bbox.s
        self.latitude_max=bbox.n
        self.longitude_min=bbox.w
        self.longitude_max=bbox.e
        self.out_dir=os.path.dirname(fout)
        self.out_name=os.path.basename(fout)
        self.variable=variables



class Uri(UriBase):
    def __init__(self,Mopts):
        self.opts=Mopts
        url=os.path.join(Mopts.out_dir,Mopts.out_name)
        super().__init__(url)

    def updateModTime(self):
        """Requests data description from the motu service"""
        self.opts.describe=True
        execute_request(self.opts)
        self.opts.describe=False


        #read and parse xml
        xml=XMLTree.parse(os.path.join(self.opts.out_dir,self.opts.out_name))
        trange=xml.find('timeCoverage')
        self.lastmod=isoParser(trange.attrib['end']).replace(tzinfo=None)
        #hack (change outname back to nc suffix)
        self.opts.out_name=self.opts.out_name.replace('.xml','.nc')


    def  getkbSize(self):
        self.opts.size=True
        execute_request(self.opts)
        self.opts.size=False
        xml=XMLTree.parse(os.path.join(self.opts.out_dir,self.opts.out_name))
        kbsize=float(xml.getroot().attrib['size'])
        self.opts.out_name=self.opts.out_name.replace('.xml','.nc')
        return kbsize


    def download(self,direc,check=False,gzip=False,outfile=None):

        #check whether the file exists and retrive thelast update date


        if outfile:
            self.opts.out_name=outfile


        fout=os.path.join(direc, self.opts.out_name)

        uri=UriFile(fout)

        if check and os.path.exists(fout):
            self.updateModTime()

            if self.lastmod <= uri.lastmod:
                logging.info("No need to download file %s"%(fout))
                return uri,False


        self.opts.out_dir=direc

        logging.info("Downloading %s"%(fout))
        execute_request(self.opts)
        return uri,True


# class Crawler(CrawlerBase):
#     """Crawls a motu server (e.g. as used by copernicus)"""
#     def __init__(self,rooturl,auth,service):
#         self.auth=auth
#         self.service=service
#         super().__init__(rooturl)
#
#     def uris(self):
#         """Retrieve the avallable  datasets of a certain service"""
#         pass
#
#     def getCatalog(self):
#         cat=http(self.rooturl+"?action=listcatalog&service="+self.service,auth=self.auth)
#         return cat.buffer()
#     # def buildURL(self):
#         # http: // localhost: 8080 / motu - web / Motu?action = listcatalog & service = HR_MOD - TDS



