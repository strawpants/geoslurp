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
import time
import os,sys
from concurrent.futures import ThreadPoolExecutor

class ftpProvider():
    """Class which encapsulates the retrieval of data from ftp"""
    def __init__(self,rooturl,maxconnections=4):
        """Sets the root url"""
        self.rooturl=rooturl
        self.curl=pycurl.Curl()
        self.curl.setopt(self.curl.URL,rooturl)
        self.maxc=maxconnections
        if not bool(re.match('^ftp://',rooturl)):
            raise Exception("URL does not seem to be a ftp address")

        

    def getftplist(self,pattern):
        """Retrieve a list of files with modification dates obeying a pattern"""
        regexdate=re.compile(b'((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[0-9]{2} +[0-9]{4})')

        regexthisyear=re.compile(b'((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[0-9]{2} +[0-9]{2}:[0-9]{2})')
        #regexdatethisyear
        buf=BytesIO() 
        self.curl.setopt(self.curl.WRITEDATA,buf)
        self.curl.perform()
        outlist=[]
        for ln in buf.getvalue().splitlines():
            fname=ln.decode('utf-8').split()[-1]
            #only apply the pattern to the last column
            tmp=re.search(pattern,fname)
            if tmp:
                t=None
                #try to parse the date from the buffer line
                match=regexdate.search(ln)
                if match:
                    t=datetime.datetime.strptime(match.group(1).decode('utf-8'),'%b %d %Y')
                else:
                    #try to see if the date contains HH:MM (this year)
                    match=regexthisyear.search(ln)
                    if match:
                        t=datetime.datetime.strptime(match.group(1).decode('utf-8'),'%b %d %H:%M')
                        t=t.replace(year=datetime.datetime.now().year)
                if t == None:
                    #one can tries to get this information through the header informatio too (slower
                
                    crl=pycurl.Curl()
           #         crl.setopt(crl.VERBOSE,1)
                    crl.setopt(crl.URL,os.path.join(self.rooturl,fname))
                    #let puycurl shut up on server messages
                    crl.setopt(crl.WRITEFUNCTION,lambda x: None)
                    crl.setopt(crl.OPT_FILETIME,1)
                    crl.setopt(crl.NOBODY,1)
                    result=crl.perform()
                    t0=datetime.datetime(1970,1,1,0,0,0)
                    t=t0+datetime.timedelta(0,crl.getinfo(pycurl.INFO_FILETIME))
                #add a tuple with modification time and file url to mathing list
                outlist.append((t,fname))
        return outlist 
    
    def downloadFile(self,fid,filen,log=None):
        """Retrieves a file from the root url"""
        if log:
            print("Downloading: "+filen,file=log)
        crl=pycurl.Curl()
        crl.setopt(crl.URL,os.path.join(self.rooturl,filen))
        crl.setopt(crl.WRITEDATA,fid)
        crl.perform()
    
    def downloadFileByName(self,fileout:str,filen,log=None,modtime=None):
            with open(fileout,'wb') as fid: 
                self.downloadFile(fid,filen,log)
            #change modification and access time to that provided by the ftp server
            if modtime != None:
                mtime=time.mktime(modtime.timetuple())
                os.utime(fileout,(mtime,mtime))

    def updateFiles(self,outdir,pattern,log=None):
        """Download/Update files in a given directory (returns a list of updated files)
        Note the download is executed in parallel"""

        #create directory if it does not exist
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        updated=[]
        with ThreadPoolExecutor(max_workers=self.maxc) as connectionPool:
            for t,filen in self.getftplist(pattern):
                outf=os.path.join(outdir,filen) 
                try:
                    ctime=datetime.datetime.fromtimestamp(os.path.getctime(outf))
                    if t <= ctime:
                        #no need to download
                        if log:
                            print("already at newest version: "+filen,file=log)
                        continue

                except os.error: #OK file does not exist 
                    pass 
                connectionPool.submit(self.downloadFileByName,outf,filen,log,t)
                updated.append(outf)

        return updated

