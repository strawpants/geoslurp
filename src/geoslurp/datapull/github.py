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
import re
import os
import json
from geoslurp.datapull import CrawlerBase
from geoslurp.datapull.http import Uri as http
import yaml
from geoslurp.config.slurplogger import  slurplogger

class GithubFilter():
    """Filter used for testing a certain dict element"""
    def __init__(self,regexdict={"type":"blob"}):
        self.regexes={}
        for ky,regex in regexdict.items():
            self.regexes[ky]=re.compile(regex)

    def isValid(self,elem):
        """Returns True if all of the regex criteria match the elem"""
        valid=True
        for ky,regex in self.regexes.items():
            if not regex.search(elem[ky]):
                valid= False

        return valid


class Crawler(CrawlerBase):
    """Crawls a github repository fixed to a certain commit"""
    def __init__(self, reponame,commitsha=None,filter=GithubFilter(),followfilt=GithubFilter({"type":"tree"}),oauthtoken=None):
        #construct the catalog url
        if commitsha:
            catalogurl="https://api.github.com/repos/"+reponame+"/git/trees/"+commitsha
        else:
            catalogurl="https://api.github.com/repos/"+reponame+"/git/trees"
        super().__init__(catalogurl)
        self.filter=filter
        self.followFilter=followfilt
        self.repo=reponame
        self.token=oauthtoken

    def getSubTree(self,url):
        if self.token:
            #add the api token to the header
            headers=[f"Authorization: token {self.token}"]
        else:
            headers=None
        return json.loads(http(url,headers=headers).buffer().getvalue())

    def uris(self,depth=10):
        """Construct Uris from tree nodes"""
        for item in self.treeitems(depth=depth):
            yield http(item["url"])    
        #add an additional elemet to keep track of the fullpath
        # for elem in self.treeitems(depth=depth):
        #     print(os.path.join(elem["dirpath"],elem['path']),elem['url'])



    def treeitems(self,rootelem=None,depth=10,dirpath=None):
        """ generator which recursively list all elements in a git tree"""


        if depth == 0:
            # signals a stopiteration
            return
        else:
            depth-=1

        #set rootelem and dirpath upon first entry
        if not rootelem:
            rootelem=self.getSubTree(url=self.rooturl)

        if not dirpath:
            dirpath=self.repo
        for treelem in rootelem['tree']:

            if self.filter.isValid(treelem):
                treelem["dirpath"]=dirpath
                #modify url to link to a arw github file

                treelem['url']="https://github.com/"+self.repo+"/raw/master"+treelem['dirpath'].replace(self.repo,"")+"/"+treelem["path"]
                yield treelem
                continue

            if self.followFilter.isValid(treelem):
                #recurse through subtree
                subtree=self.getSubTree(treelem["url"])
                yield from self.treeitems(subtree,depth,os.path.join(dirpath,treelem["path"]))

def cachedGithubCatalogue(reponame,cachedir=".",commitsha=None,gfilter=GithubFilter(),gfollowfilter=GithubFilter({"type":"tree"}),depth=2,ghtoken=None):
    """Caches the result of a github result for later reuse"""

    cachedCatalog=os.path.join(cachedir,reponame.replace("/","_")+".yaml")
    catalog={}
    if os.path.exists(cachedCatalog):
        #check whether the commit sha agrees when explicitly specified
        if commitsha:
            #read catalog from yaml file
            with open(cachedCatalog, 'r') as fid:
                catalog=yaml.safe_load(fid)
            # import pdb;pdb.set_trace() 
            if catalog["commitsha"] != commitsha:
                #trigger a new download
                catalog={}
        else:
            #always download a newer version
            catalog={}

    if catalog:
        slurplogger().info("using cached github catalogue %s"%(cachedCatalog))
    else:
        slurplogger().info("downloading github catalogue to cache %s"%(cachedCatalog))
        #retrieve from github and store for later use
        crwl=Crawler(reponame,commitsha=commitsha,
                       filter=gfilter,
                       followfilt=gfollowfilter,
                       oauthtoken=ghtoken)

        catalog={"Description":"Cached github crawler results","rooturl":crwl.rooturl
                ,"commitsha":commitsha,"datasets":[]}
        # import pdb;pdb.set_trace()
        for item in crwl.treeitems(depth=depth):
            catalog["datasets"].append({"path":os.path.join(item["dirpath"],item["path"]),"url":item["url"]})
        
        #save the results to a cached file
        with open(cachedCatalog,'w') as fid:
            yaml.dump(catalog,fid,default_flow_style=False)
    
    return catalog

