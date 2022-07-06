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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022

import re
from datetime import datetime
import os
from geoslurp.datapull import UriFile,setFtime
from geoslurp.datapull import CrawlerBase
from geoslurp.config.slurplogger import slurplog
import paramiko


def sshconnect(auth,url=None):
    """Connect to a ssh server and returns a initialized paramiko sshclient object"""
    if url is None:
        url=auth.url

    if url[-1] != '/':
        url+='/'

    # Extract server name directory and port from url
    if not bool(re.match('^sftp://',url)):
        raise Exception("sftp url does not seem to be a valid secure-ftp address")


    #url has the format: sftp://servername[:port]/possible/sub/dir/
    server,subdir=re.sub(':[0-9]+','',url[7:]).split("/",1)
    pmatch=re.search(":([0-9]+)",url)
    if pmatch is None:
        port=22
    else:
        port=int(pmatch.group(1))

    user = auth.user
    password = auth.passw

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(server,port,user,password,look_for_keys=False,allow_agent=False)
    sftp=ssh_client.open_sftp() 
    #possibly change directory
    if bool(subdir):
        sftp.chdir(subdir)

    return sftp


class UriSftp:
    def __init__(self,url,lastmod=None,subdirs='',auth=None,sftpcon=None):
        #extract filename from url
        self.rpath=os.path.basename(url)
        if sftpcon:
            self.sftpconnection=sftpcon
        else:
            self.sftpconnection=sshconnect(url=os.path.dirname(url),auth=auth)
        self.lastmod=lastmod
        self.subdirs=subdirs
        self.url=url


    def download(self,direc,check=False,outfile=None,continueonError=False,restdict=None):
        """Download file into directory and possibly check the modification time
        :param check : check whether the file needs updating
        :param gzip: additionally gzips the file (adds .gz to file name)
        :param continueonError (bool): don't raise an exception when a download error occurrs
        """
        
        #setup the output uri
        if outfile:
            outf=os.path.join(direc,self.subdirs,outfile)
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

        stat=self.sftpconnection.stat(self.rpath)
        mtime=datetime.fromtimestamp(stat.st_mtime)
        self.sftpconnection.get(self.rpath,outf)
        #set the modification time to match the server
        setFtime(outf,mtime)
        
        return uri,True


class CrawlerSftp(CrawlerBase):
    """Crawler for secure-ftp directories"""
    def __init__(self,url,pattern='.*',auth=None):
        super().__init__(url)
        self.sftpconnection=sshconnect(url=url,auth=auth)

        self.pattern=pattern

    def ls(self,subdirs=''):
        if bool(subdirs):
            self.sftpconnection.chdir(subdirs)

        for name in self.sftpconnection.listdir():
            stat=self.sftpconnection.stat(name)
            mtime=datetime.fromtimestamp(stat.st_mtime)
            yield name,mtime
    

    def uris(self, check=False,subdirs=''):
        # """Generate a list files in a directory and return a list of uri"""
        for name,t in self.ls(subdirs):
            # #only apply the pattern to the last column
            if re.search(self.pattern,name):
                yield UriSftp(url=name,sftpcon=self.sftpconnection,lastmod=t)

