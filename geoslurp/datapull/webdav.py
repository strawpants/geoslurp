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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020 (a webdav version without easywebdav dependency)

import re
import os
import time
from datetime import datetime
from geoslurp.datapull import UriBase, UriFile, setFtime,curlDownload
from geoslurp.datapull import CrawlerBase
from geoslurp.config.slurplogger import slurplog
from dateutil.parser import parse
from io  import BytesIO
from lxml import etree as XMLTree
from dateutil.parser import parse as isoParser

class Crawler(CrawlerBase):
    """Webdav Crawler (list content of a directory)"""
    pattern=None
    def __init__(self,rooturl,pattern,auth):
        if not rooturl.endswith('/'):
            rooturl+='/'
        super().__init__(rooturl)
        self.pattern=pattern
        self.auth=auth

    def uris(self):
        # #extract protocol from url
        proto,url=self.rooturl.split('://')
        # #also strip off directory
        baseurl, tmp=url.split('/', 1)
        baseurl=proto+"://"+baseurl
        
        regex=re.compile(self.pattern)
        
        buffer=BytesIO()
        buffer.write(b'<?xml version="1.0"?>'
            b'<a:propfind xmlns:a="DAV:">'
            b'<a:prop><a:getlastmodified/></a:prop>'
            b'</a:propfind>')
    
        #retrieve the directory listing as xml by making a PROPFIND HTTP request tot eh webdav server
        xmlout=BytesIO()
        curlDownload(self.rooturl,xmlout,auth=self.auth,headers=["Depth: 1"],customRequest="PROPFIND",upfid=buffer)
        xmlroot=XMLTree.fromstring(xmlout.getvalue())
        #walk through the xml tree and gather files with their modification dates
        for xelem in xmlroot.iterfind('{DAV:}response'):
            url=xelem.find('{DAV:}href').text
            lastmod=xelem.find('{DAV:}propstat/{DAV:}prop/{DAV:}getlastmodified').text
            mdate=isoParser(lastmod).replace(tzinfo=None)
            if not regex.search(url):
                continue
            yield UriBase(baseurl+url,lastmod=mdate,auth=self.auth)
