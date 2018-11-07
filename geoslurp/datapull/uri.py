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

def timeFromStamp(stamp):
    tinfo = stamp

    if tinfo == -1:
        dt=datetime.now()
    else:
        t0=datetime.datetime(1970,1,1,0,0,0)
        dt=t0+datetime.timedelta(0,tinfo)
    return dt



class UriBase():
    """Base class to store uri resource"""
    url=None
    lastmod=None
    auth=None #link to a certain authentification alias
    buf=None # possibly an in memory buffer or open file to hold the resource
    def __init__(self,url,lastmod=None):
        self.url=url
        self.lastmod=lastmod

    def updateModTime(self):
        """retrieves the last modification time"""
        pass

    def download(self,direc, check=False):
        """Download file to a certain directory and returns the new UriBase"""
        pass

class UriFile(UriBase):
        def __init__(self,url,lastmod=None):
           super().__init__(url,lastmod)

        def updateModTime(self):
            self.lastmod = datetime.fromtimestamp(os.path.getmtime(self.url))
            return self.lastmod

class UriHttp(UriBase):
    buf=None
    def __init__(self,url):
        super().__init__(url)
        if not bool(re.match('^https?://',url)):
            raise Exception("URL does not seem to be a valid http(s) address")

    def updateModTime(self):
        """Tries to retrieve the last modification time of a file"""
        crl=pycurl.Curl()
        crl.setopt(pycurl.URL,self.url)
        #note: not all servers support this query with NOBODY set to 1
        crl.setopt(pycurl.NOBODY, 1)
        crl.perform()
        self.lastmod=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
        return self.lastmod

    def download(self,direc,buffer=False,check=False):
        """Download file into directory and possibly check the modification time"""
        #setup the output uri
        if buffer:
            uri=UriFile(url='').Buffer()
        else:
            uri=UriFile(url=os.path.join(direc,os.path.basename(self.url)))
        if check and os.path.exists(uri.url):
            # print("File already Downloaded, skipping")
            #quick return (file is already downloaded)
            return uri

        crl=pycurl.Curl()
        crl.setopt(pycurl.URL,self.url)
        crl.setopt(pycurl.FOLLOWLOCATION, 1)
        if buffer:
            crl.setopt(pycurl.WRITEDATA,uri.buf)
            crl.perform()
            self.lastmod=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
            uri.lastmod=self.lastmod
        else:
            with open(uri.url,'wb') as fid:
                crl.setopt(pycurl.WRITEDATA,fid)
                crl.perform()

            self.lastmod=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
            #change modification and access time to that provided by the http server (does not always work)
            mtime=time.mktime(self.lastmod.timetuple())
            os.utime(uri.url,(mtime,mtime))
            uri.lastmod=self.lastmod


        return uri



# class UriFtp(UriBase):
#     def __init__(self,url):
#         super().__init__(url)
#         if not bool(re.match('^ftp?://',url)):
#             raise Exception("URL does not seem to be a valid ftp address")


class UriCollection():
    """Holds various alternatives to a resource"""
    def __init__(self):
        self.data=[]
