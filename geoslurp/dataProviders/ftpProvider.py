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
import pycurl,re
from io import BytesIO
import datetime
import os
class ftpProvider():
    """Class which encapsulates the retrieval of data from ftp"""
    def __init__(self,rooturl):
        """Sets the root url"""
        self.rooturl=rooturl
        self.curl=pycurl.Curl()
        self.curl.setopt(self.curl.URL,rooturl)
        if not bool(re.match('^ftp://',rooturl)):
            raise Exception("URL does not seem to be a ftp address")

        self.t0=datetime.datetime(1970,1,1,0,0,0)
        

    def getftplist(self,pattern):
        """Retrieve a list of files with modification dates obeying a pattern"""
        buf=BytesIO() 
        self.curl.setopt(self.curl.WRITEDATA,buf)
        self.curl.perform()
        outlist=[]
        for ln in buf.getvalue().splitlines():
            fname=ln.decode('utf-8').split()[-1]
            #only apply the pattern to the last column
            tmp=re.search(pattern,fname)
            if tmp:
                #request file modification time
                crl=pycurl.Curl()
                crl.setopt(crl.URL,self.rooturl+fname)
                crl.setopt(crl.NOBODY,1)
                #let puycurl shut up on server messages
                crl.setopt(crl.WRITEFUNCTION,lambda x: None)
                crl.setopt(crl.OPT_FILETIME,1)
                crl.perform()
                t=self.t0+datetime.timedelta(0,crl.getinfo(pycurl.INFO_FILETIME))
                #add a tuple with modification time and file url to mathing list
                outlist.append((t,fname))
        return outlist 
    
    def downloadFile(self,fid,filen):
        """Retrieves a file from the root url"""
        crl=pycurl.Curl()
        crl.setopt(crl.URL,os.path.join(self.rooturl,filen))
        crl.setopt(crl.WRITEDATA,fid)
        crl.perform()

