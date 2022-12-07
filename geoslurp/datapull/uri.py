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
from geoslurp.config.slurplogger import slurplog
import gzip as gz
from urllib.parse import urlencode

def findFiles(dir,pattern,since=None):
    """Generator to recursively search adirecctor (returns a generator)"""

    if not since:
        since=datetime.min

    for dpath,dnames,files in os.walk(dir):
        # for subdir in dnames:
        #     yield from findFiles(os.path.join(dir,subdir),pattern)

        for file in files:
            if re.search(pattern,file):
                fullp=os.path.join(dpath,file)

                if since < datetime.fromtimestamp(os.path.getmtime(fullp)):
                    yield fullp

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

def curlDownload(url,fileorfid,mtime=None,gzip=False,gunzip=False,auth=None,restdict=None,headers=None,customRequest=None,upfid=None,cookiefile=None):
    """
    Download  the content of an url to an open file or buffer using pycurl
    :param url: url to download from
    :param fileorfid: filename or open file or buffer
    :param mtime: explicitly set the modification time to this (usefull when modification times are not supported
    b the server)
    :param gzip: additionally gzip the file on disk (note this routine does not append \*.gz to the file name)
    :param gunzip: automatically gunzip the downloaded file
    :param auth: supply authentification data (user and passw)
    :param restdic: a set of (REST) API name-value pairs to be added to the url (provide as a dict)
    :param headers (array of header values): additionally set header elements
    :param customRequest: set a custoi request (e.g. for WEBDAV servers)
    :return: modification time of remote file
    """

    if gzip and gunzip:
        raise RuntimeError("cannot gzip and gunzip at the same time")

    if type(fileorfid) == str:
        if gunzip:
            tmpfile=os.path.join(os.path.dirname(fileorfid),"."+os.path.basename(fileorfid)+".tmp.gz")
        else:
            tmpfile=os.path.join(os.path.dirname(fileorfid),"."+os.path.basename(fileorfid)+".tmp")

        if gzip:
            #note this routine does not change the filename!!
            fid=gz.open(tmpfile,'wb')
        else:
            fid=open(tmpfile,'wb')
    else:
        fid=fileorfid

    crl=pycurl.Curl()
    #crl.setopt(pycurl.VERBOSE,1)
    crl.setopt(pycurl.USERAGENT,"curl/7.72.0")
    crl.setopt(pycurl.URL,url.replace(' ','%20'))
    crl.setopt(pycurl.FOLLOWLOCATION, 1)
    crl.setopt(pycurl.WRITEDATA,fid)
     
    if customRequest:
        crl.setopt(pycurl.CUSTOMREQUEST,customRequest)

    
    if cookiefile:
        if os.path.exists(cookiefile):
            crl.setopt(pycurl.COOKIEFILE, cookiefile)

        crl.setopt(pycurl.COOKIEJAR, cookiefile)


    if auth:
        if hasattr(auth,"ftptls"):
            #enable explicit ftp over tls
            if auth.ftptls:
    #            crl.setopt(pycurl.HTTPPROXYTUNNEL,1)
                crl.setopt(pycurl.FTP_SSL, pycurl.FTPSSL_ALL)
                crl.setopt(pycurl.FTPSSLAUTH,pycurl.FTPAUTH_TLS)
        if hasattr(auth,"trusted"):
            if auth.trusted:
                crl.setopt(pycurl.UNRESTRICTED_AUTH,1)
        if hasattr(auth,'oauthtoken'):
            #use oauth in a header to authenticate
            oauthhead=f"Authorization: Bearer {auth.oauthtoken}"
            if headers:
                headers.append(oauthead)
            else:
                headers=[oauthhead]


        else:
            #use basic authentication
            crl.setopt(pycurl.USERPWD,auth.user+":"+auth.passw)

    if headers:
        crl.setopt(pycurl.HTTPHEADER,headers)
    
    if restdict:
        crl.setopt(crl.POSTFIELDS,urlencode(restdict))
    
    if upfid:
        crl.setopt(pycurl.UPLOAD,1)
        crl.setopt(pycurl.READDATA,upfid)

    try:
        crl.perform()
    except pycurl.error as pyexc:
        # possibly remove a partly downloaded file
        if type(fileorfid) == str:       
            if os.path.exists(tmpfile):
                os.remove(tmpfile)
        raise pyexc

    modtime=timeFromStamp(crl.getinfo(pycurl.INFO_FILETIME))
    if mtime:
        #force the modification time to that provided
        modtime=mtime

    #close file if input was a filename or unzip data in the output file
    if type(fileorfid) == str:
        fid.close()
        if gunzip:
            #decompress the temporary gzipped file in the outputfile
            with open(fileorfid,'wb') as fidout:
                with gz.open(tmpfile,'rb') as gzid:
                    fidout.write(gzid.read())
            #remove the temporary file
            os.remove(tmpfile)
        else:
            # just rename temporary file
            os.rename(tmpfile,fileorfid)
        setFtime(fileorfid,modtime)

    return modtime


class UriBase():
    """Base class to store uri resource"""
    url=None
    lastmod=None
    auth=None #link to a certain authentification alias
    subdirs='' #create these subdrectories when downloading the file
    headers=None 
    def __init__(self,url,lastmod=None,auth=None,subdirs='',headers=None,cookiefile=None):
        self.url=url
        self.lastmod=lastmod
        self.auth=auth
        self.subdirs=subdirs
        self.headers=headers
        self.cookiefile=cookiefile

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

    def download(self,direc,check=False,gzip=False,gunzip=False,outfile=None,continueonError=False,restdict=None):
        """Download file into directory and possibly check the modification time
        :param check : check whether the file needs updating
        :param gzip: additionally gzips the file (adds .gz to file name)
        :param continueonError (bool): don't raise an exception when a download error occurrs
        """

        #setup the output uri
        if outfile:
            outf=os.path.join(direc,self.subdirs,outfile)
        else:
            if gzip:
                outf=os.path.join(direc,self.subdirs,os.path.basename(self.url))+'.gz'
            elif gunzip:
                #strip gz suffix
                outf=os.path.splitext(os.path.join(direc,self.subdirs,os.path.basename(self.url)))[0]
            else:
                outf=os.path.join(direc,self.subdirs,os.path.basename(self.url))



        #create directory if it does not exist
        if not os.path.exists(os.path.dirname(outf)):
            os.makedirs(os.path.dirname(outf),exist_ok=True)

        uri=UriFile(url=outf)
        if check and self.lastmod and uri.lastmod:
            if self.lastmod <= uri.lastmod:
                #no need to download the file
                slurplog.info("Already Downloaded, skipping %s"%(uri.url))
                return uri,False
        slurplog.info("Downloading %s"%(uri.url))
        try:
            if self.lastmod:
                curlDownload(self.url,uri.url,self.lastmod,gzip=gzip,gunzip=gunzip,auth=self.auth,restdict=restdict,headers=self.headers,cookiefile=self.cookiefile)
            else:
                self.lastmod=curlDownload(self.url,uri.url,gzip=gzip,gunzip=gunzip,auth=self.auth,restdict=restdict,headers=self.headers,cookiefile=self.cookiefile)
        except pycurl.error as pyexc:
            slurplog.info("Download failed, skipping %s"%(uri.url))
            if not continueonError:
                raise pyexc
        except Exception as e:
            raise e
        uri.lastmod=self.lastmod
        return uri,True

    def buffer(self):
        """Download file into a buffer (default uses curl)"""
        buf=BytesIO()
        curlDownload(self.url,buf,auth=self.auth)
        return buf

class UriFile(UriBase):
        def __init__(self,url,lastmod=None):
            super().__init__(url,lastmod)
            #Lets set lastmod straight away if the file exists
            if os.path.exists(url) and not lastmod:
                self.updateModTime()
            

        def updateModTime(self):
            self.lastmod = datetime.fromtimestamp(os.path.getmtime(self.url))
            return self.lastmod

        def buffer(self):
            pass


