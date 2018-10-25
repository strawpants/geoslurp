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
import os
class httpProvider():
    """Class which encapsulates the retrieval of data from http(s)"""
    def __init__(self,rooturl):
        """Sets the root url"""
        self.rooturl=rooturl
        # self.curl=pycurl.Curl()
        # self.curl.setopt(self.curl.URL,rooturl)
        if not bool(re.match('^https?://',rooturl)):
            raise Exception("URL does not seem to be a valid http(s) address")
    
    def getLastModTime(self,filen=None):
        """Tries to retrieve the last modification time of a file"""
        crl=pycurl.Curl()
        crl.setopt(pycurl.URL,self.buildURL(filen))
        crl.setopt(pycurl.NOBODY, 1)
        crl.perform()
        return crl.getinfo(pycurl.INFO_FILETIME)
    
    def downloadFile(self,fid,filen=None):
        """Retrieves a file from the root url appended with a filename """
        crl=pycurl.Curl()
        
        crl.setopt(pycurl.URL,self.buildURL(filen))
        crl.setopt(pycurl.FOLLOWLOCATION, 1)
        crl.setopt(pycurl.WRITEDATA,fid)
        crl.perform()

    def downloadFileByName(self,fileout:str,filen=None):
        with open(fileout,'wb') as fid:
            self.downloadFile(fid,filen=filen)

    def buildURL(self,filen):
        """Build an url by appending to the root or taking the full input url"""
        url=self.rooturl
        if not filen:
            #quick return if downlaod from rooturl is desires
            return url
        if bool(re.match('^https?://',filen)):
            #use complete URL
            url=filen
        else:
            #append to root url
            if filen:
                url=os.path.join(self.rooturl,filen)
        return url
