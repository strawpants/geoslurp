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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from geoslurp.dataset import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.datapull.github import Crawler as ghCrawler
from geoslurp.datapull.github import GithubFilter as ghfilter
from datetime import datetime
import json
from zipfile import ZipFile
import os
import tempfile
import yaml

class NaturalEarthBase(OGRBase):
    """Base class for  a Natural Earth dataset (downloads from github) """
    url=None
    path=None
    def __init__(self,scheme):
        super().__init__(scheme)
        self.ogrfile=os.path.join(self.cacheDir(),os.path.basename(self.path))
        self.gtype='GEOMETRY'
    def pull(self):
        """Pulls the geojson data from the internet and unpacks it in the cache directory"""
        uri=http(self.url)
        basename=os.path.basename(self.path)
        uri.download(direc=self.cacheDir(),outfile=basename)


def NaturalEarthClassFactory(clsName,catentry):
    return type(clsName, (NaturalEarthBase,), {"url":catentry["url"],"path":catentry["path"]})

def getNaturalEarthDict(conf):
    """retrieves the available natural earth datasets"""
    currentversion='v4.1.0' # note this correspond to a specific release (the sha hash is taken from the commit)

    cachedCatalog=os.path.join(conf.getDir("tmpfiles","CacheDir"),"naturalearthCatalog"+currentversion+".yaml")

    if os.path.exists(cachedCatalog):
        #read catalog from yaml file
        with open(cachedCatalog, 'r') as fid:
            catalog=yaml.safe_load(fid)
    else:
        #retrieve from github and store for later use
        try:
            cred=conf.authCred("github")
            token=cred.oauthtoken
        except:
            token=None

        crwl=ghCrawler("nvkelso/natural-earth-vector","bf7720b54dd9ac2d4d7f735174901b3862b5362a",
                       filter=ghfilter({"type":"blob","path":"\.geojson"}),
                       followfilt=ghfilter({"type":"tree","path":"geojson"}),
                       oauthtoken=token)

        catalog={"Description":"Natural Earth vector data catalog version: "+currentversion,"datasets":[]}
        for item in crwl.treeitems(depth=2):
            catalog["datasets"].append({"path":os.path.join(item["dirpath"],item["path"]),"url":item["url"]})
            # basename=os.path.basename(item["path"]).split(".")
            # if not basename[0] in catalog["datasets"]:
            # else:
            #     catalog["datasets"][basename[0]].update({basename[1]:item["url"]})

        with open(cachedCatalog,'w') as fid:
            yaml.dump(catalog,fid,default_flow_style=False)
    outdict={}
    #create a dictionary of types
    for entry in catalog["datasets"]:
        clsname=os.path.basename(entry["path"]).split(".")[0]
        outdict[clsname]=NaturalEarthClassFactory(clsname,entry)


    return outdict

