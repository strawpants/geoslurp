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
from concurrent.futures import ThreadPoolExecutor

#python 3 hacks for easywebdav (see https://stackoverflow.com/questions/26130644/how-to-overcome-python-3-4-nameerror-name-basestring-is-not-defined)
easywebdav.basestring = str
easywebdav.client.basestring = str

class WebdavProvider():
    """
    Get files from a webdav capable server
    """
    def __init__(self, rooturl, user=None, passw=None):
        """
        Initialize the webdav
        :param rooturl:  top url to start off from
        :param user: Username
        :param passw: password
        """

        #extract protocol from url
        proto,url=rooturl.split('://')
        #also strip off direectory
        url, direc=url.split('/', 1)
        self.webdav = easywebdav.connect(url, username=user, password=passw, protocol=proto)
        #change directory
        self.webdav.cd(direc)
        #maximum amount fo connections for downloading
        self.maxc=4

    def ls(self,arg='.'):
        return self.webdav.ls(arg)

    def downloadFileByName(self,fileout:str,filen,log=None,modtime=None):
        """Download file by name and set modification time to remote modification"""
        print("Downloading %s"%(os.path.basename(filen)),file=log)
        self.webdav.download(filen, fileout)

        #change modification and access time to that provided by the ftp server
        if modtime != None:
            mtime=time.mktime(modtime.timetuple())
            os.utime(fileout,(mtime,mtime))

    def updateFiles(self,outdir,pattern,log=None):
        """
        Download files but only update those which have newer remote versions
        :param outdir: Where to put/check for downloaded files
        :param pattern: pattern to apply to directory listing
        :param log: write messages here
        :returns: nothing
        """

        updated=[]
        regex=re.compile(pattern)

        with ThreadPoolExecutor(max_workers=self.maxc) as connectionPool:
            #list remote files and loop over them
            for remf in self.ls():
                if not regex.search(remf.name):
                    continue

                #get modification time of the remote file
                t=datetime.strptime(remf.mtime, "%a, %d %b %Y %H:%M:%S %Z")

                #check if file already exists and whether it is too old
                fbase=os.path.basename(remf.name)

                outf=os.path.join(outdir,fbase)
                try:
                    ctime=datetime.fromtimestamp(os.path.getctime(outf))
                    if t <= ctime:
                        #no need to download
                        if log:
                            print("already at newest version: "+fbase,file=log)
                        continue

                except os.error: #OK file does not exist
                    pass

                #download file
                connectionPool.submit(self.downloadFileByName,outf,remf.name,log,t)
                updated.append(fbase)
        return updated