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

from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.datapull.github import Crawler as ghCrawler
from geoslurp.datapull.github import GithubFilter as ghfilter
from geoslurp.config.register import geoslurpregistry
import json
from zipfile import ZipFile
import os
import tempfile
import yaml

class geoshapeBase(OGRBase):
    """Base class for a geoshapes dataset (selection of handrawn shapes) (downloads from github) """
    url=None
    path=None
    scheme='globalgis'
    version=(0,0,0)
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),os.path.basename(self.path))
        self.gtype='GEOMETRY'
    def pull(self):
        """Pulls the geojson data from github and unpacks it in the cache directory"""
        uri=http(self.url)
        basename=os.path.basename(self.path)
        uri.download(direc=self.cacheDir(),outfile=basename)


def GeoshapeClassFactory(clsName,val):
    return type(clsName, (geoshapeBase,), {"url":val["url"],"description":val["Description"],"path":val["path"]})

def getGeoshapesDsets(conf):
    """retrieves the available geoshapes datasets"""
    currentversion='v1' # note this correspond to a specific release (the sha hash is taken from the commit)
    catalogfile="geoshapesCatalog"+currentversion+".yaml"
    cachedir=conf.getDir("geoshapes","CacheDir")
    cachedCatalog=os.path.join(cachedir,catalogfile)

    #get the first entry (shoudl only be one)
    # import pdb;pdb.set_trace()
    if not os.path.exists(cachedCatalog):
        uri=http("https://raw.githubusercontent.com/strawpants/geoshapes/master/inventory.yaml").download(direc=cachedir,outfile=catalogfile)

    with open(cachedCatalog,'r') as fid:
        catalog=yaml.safe_load(fid)
    out=[]
    
    # add urls
    for key,val in catalog.items():
        catalog[key]["url"]="https://raw.githubusercontent.com/strawpants/geoshapes/master/"+key
        catalog[key]["path"]=key

    #create a list of datasets
    for key,val in catalog.items():
        clsname=os.path.basename(key).split(".")[0]
        out.append(GeoshapeClassFactory(clsname,val))


    return out


geoslurpregistry.registerDatasetFactory(getGeoshapesDsets)



