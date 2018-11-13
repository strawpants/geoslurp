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

import os
from datetime import datetime
import pycurl,re
import time
from io  import BytesIO
import logging
import gzip as gz

def timeFromStamp(stamp):
    tinfo = stamp

    if tinfo == -1:
        dt=datetime.now()
    else:
        t0=datetime.datetime(1970,1,1,0,0,0)
        dt=t0+datetime.timedelta(0,tinfo)
    return dt

def setFtime(file,modTime=None):
    """change modification and access time of a file"""
    if modTime:
        mtime=time.mktime(modTime.timetuple())
        os.utime(file,(mtime,mtime))

def curlDownload(url,fileorfid,mtime=None,gzip=True):
    """
    Download  the content of an url to an open file or buffer using pycurl
    :param url: url to download from
    :param fileorfid: filename or open file or buffer
    :param mtimee: explicitly set the modification time to this (usefull when modification times are not supported
    b the server)
    :param gzip: additionally gzip the file on disk
    :return: modification time of remote file
    """
    if type(fileorfid) == str:
        if gzip:
            #note this routine does not change the filename!!
            fid=gz.open(fileorfid,'wb')
        else:
            fid=open(fileorfid,'wb')
    else:
        fid=fileorfid

    crl=pycurl.Curl()
    crl.setopt(pycurl.URL,url)
    crl.setopt(pycurl.FOLLOWLOCATION, 1)
    crl.setopt(pycurl.WRITEDATA,fid)
    crl.perform()
    modtime=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
    if mtime:
        #force the modification time to that provided
        modtime=mtime

    #close file if input was a filename
    if type(fileorfid) == str:
        fid.close()
        setFtime(fileorfid,modtime)

    return modtime


class UriBase():
    """Base class to store uri resource"""
    url=None
    lastmod=None
    auth=None #link to a certain authentification alias
    def __init__(self,url,lastmod=None,auth=None):
        self.url=url
        self.lastmod=lastmod
        self.auth=auth

    def updateModTime(self):
        """Tries to retrieve the last modification time of a file
        Note: his is often not supported by the server"""
        crl=pycurl.Curl()
        crl.setopt(pycurl.URL,self.url)
        #note: not all servers support this query with NOBODY set to 1
        crl.setopt(pycurl.NOBODY, 1)
        crl.setopt(crl.WRITEFUNCTION,lambda x: None)
        crl.perform()
        self.lastmod=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
        return self.lastmod

    def download(self,direc,check=False,gzip=False):
        """Download file into directory and possibly check the modification time
        ":param gzip: additionally gzips the file (adds .gz to file name)"""
        #setup the output uri
        if gzip:
            outf=os.path.join(direc,os.path.basename(self.url))+'.gz'
        else:
            outf=os.path.join(direc,os.path.basename(self.url))

        uri=UriFile(url=outf)
        if check and self.lastmod and uri.lastmod:
            if self.lastmod <= uri.lastmod:
                #no need to download the file
                logging.info("Already Downloaded, skipping %s"%(uri.url))
                return uri,False
        logging.info("Downloading %s"%(uri.url))
        if self.lastmod:
            curlDownload(self.url,uri.url,self.lastmod,gzip=gzip)
        else:
            self.lastmod=curlDownload(self.url,uri.url)
        uri.lastmod=self.lastmod
        return uri,True

    def buffer(self):
        """Download file into a buffer (default uses curl)"""
        buf=BytesIO()
        curlDownload(self.url,buf)
        return buf

class UriFile(UriBase):
        def __init__(self,url,lastmod=None):
            super().__init__(url,lastmod)
            #Lets set lastmod straight away if the file exists
            if os.path.exists(url):
                self.updateModTime()

        def updateModTime(self):
            self.lastmod = datetime.fromtimestamp(os.path.getmtime(self.url))
            return self.lastmod

        def buffer(self):
            pass


