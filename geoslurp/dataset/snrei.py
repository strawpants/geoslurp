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
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019
from geoslurp.dataset import DataSet
from geoslurp.datapull.http import Uri as http
from geoslurp.config.catalogue import geoslurpCatalogue
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String,MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP
from geoslurp.datapull.github import Crawler as ghCrawler
from geoslurp.datapull.github import GithubFilter as ghfilter
from geoslurp.config.slurplogger import slurplogger

from geoslurp.datapull import UriFile
from geoslurp.datapull import findFiles
import gzip
import re
import os

def lloveMetaExtractor(uri):
    """extract some metainfo from the load Lovenumber file"""
    #extract maximum degree from file and heuristically derive loadtype from the filename)
    if re.search("body",uri.url):
        ltype="body"
    else:
        ltype="surface"
    
    nmax=0
    reentry=re.compile('^ *[0-9]')
    with gzip.open(uri.url,'rt') as fid:
        for ln in fid:
            if reentry.search(ln):
                nmax=max(nmax,int(ln.split()[0]))
                    
    
    meta={"name":os.path.basename(uri.url).replace(".love.gz",""),"lastupdate":uri.lastmod,
            "uri":uri.url,"loadtype":ltype,"nmax":nmax}


    return meta

schema='earthmodels'
LLoveTBase=declarative_base(metadata=MetaData(schema=schema))

class LLoveTable(LLoveTBase):
    """Defines the Load Love number PostgreSQL table"""
    __tablename__='llove'
    id=Column(Integer,primary_key=True)
    name=Column(String,unique=True)
    loadtype=Column(String)
    lastupdate=Column(TIMESTAMP)
    nmax=Column(Integer)
    uri=Column(String)

class LLove(DataSet):
    """Class for registering load love numbers (downloads from github) """
    scheme=schema
    version=(0,0,0)
    table=LLoveTable
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
    def pull(self):
        """Pulls the dataset from github and unpacks it in the cache directory"""
        #crawls the github repository for Load Love numbers

        reponame="strawpants/snrei"
        commitsha="e61d3e2a9eb328d48f56f5aa73fa2aaba60f1d5c"
    
        try:
            cred=self.conf.authCred("github")
            token=cred.oauthtoken
        except:
            token=None
        # import pdb;pdb.set_trace() 
        ghcrawler=ghCrawler(reponame,commitsha=commitsha,
                           filter=ghfilter({"type":"blob","path":"\.love"}),
                           followfilt=ghfilter({"type":"tree","path":"Love"}),
                           oauthtoken=token)
        
        #download all datasets
        ghcrawler.parallelDownload(self.dataDir(),check=True,maxconn=3,gzip=True)

    def register(self):
        slurplogger().info("Building file list..")
        files=[UriFile(file) for file in findFiles(self.dataDir(),'.*love',self._dbinvent.lastupdate)]

        if len(files) == 0:
            slurplogger().info("LLove: No new files found since last update")
            return

        filesnew=self.retainnewUris(files)
        if len(filesnew) == 0:
            slurplogger().info("LLove: No database update needed")
            return
        #loop over files
        for uri in filesnew:
            self.addEntry(lloveMetaExtractor(uri))
        self.updateInvent()


geoslurpCatalogue.addDataset(LLove)
