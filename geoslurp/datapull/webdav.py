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

import easywebdav
import re
import os
import time
from datetime import datetime
from geoslurp.datapull import UriBase, UriFile, setFtime
from geoslurp.datapull import CrawlerBase
from geoslurp.config.slurplogger import slurplogger

#python 3 hacks for easywebdav (see https://stackoverflow.com/questions/26130644/how-to-overcome-python-3-4-nameerror-name-basestring-is-not-defined)
easywebdav.basestring = str
easywebdav.client.basestring = str

class Uri(UriBase):
    """"Webdav URI"""
    webdav=None
    def __init__(self,rooturl,lastmod=None,auth=None):
        super().__init__(rooturl,lastmod,auth=auth)
        #extract protocol from url
        self.proto,url=rooturl.split('://')
        #also strip off directory
        self.baseurl, tmp=url.split('/', 1)
        self.direc=os.path.dirname(tmp)
        self.fname=os.path.basename(tmp)

    def connect(self):
        self.webdav = easywebdav.connect(self.baseurl, username=self.auth.user, password=self.auth.passw, protocol=self.proto)
        #change directory
        self.webdav.cd(self.direc)
    def ls(self):
        if not self.webdav:
            self.connect()
        return self.webdav.ls('.')

    def download(self,direc,check=False,gzip=False,outfile=None,continueonError=False):
        
        if not self.webdav:
            self.connect()

        if outfile:
            outf=os.path.join(direc,outfile)
        else:
            outf=os.path.join(direc,self.fname)

        uri=UriFile(url=outf)

        if check and self.lastmod and uri.lastmod:
            if self.lastmod <= uri.lastmod:
                #no need to download the file
                slurplogger().info("Already Downloaded, skipping %s"%(uri.url))
                return uri,False

        slurplogger().info("Downloading %s"%(uri.url))
        self.webdav.download(self.fname, uri.url)

        #change modification and access time to that provided by the ftp server
        setFtime(uri.url,self.lastmod)
        return uri,True

    def subUri(self,remf):
        """Returns a webdav URI derived from this one"""
        tmp=self

        #get modification time of the remote file
        tmp.lastmod=datetime.strptime(remf.mtime, "%a, %d %b %Y %H:%M:%S %Z")

        #check if file already exists and whether it is too old
        tmp.fname=os.path.basename(remf.name)
        tmp.url=self.proto+"://"+self.baseurl+"/"+self.direc+"/"+tmp.fname
        return tmp

class Crawler(CrawlerBase):
    """Webdav Crawler"""
    pattern=None
    webdavroot=None
    def __init__(self,rooturl,pattern,auth):
        if not rooturl.endswith('/'):
            rooturl+='/'
        super().__init__(rooturl)
        self.webdavroot=Uri(rooturl,auth=auth)
        self.pattern=pattern

    def uris(self):
        regex=re.compile(self.pattern)
        for remfile in self.webdavroot.ls():
            if not regex.search(remfile.name):
                continue

            yield self.webdavroot.subUri(remfile)

