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

from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.datapull.github import Crawler as ghCrawler
from geoslurp.datapull.github import GithubFilter as ghfilter
from geoslurp.datapull.github import cachedGithubCatalogue

from geoslurp.config.catalogue import geoslurpCatalogue
import json
from zipfile import ZipFile
import os
import tempfile
import yaml

class NaturalEarthBase(OGRBase):
    """Base class for  a Natural Earth dataset (downloads from github) """
    url=None
    path=None
    scheme='globalgis'
    version=(4,1,0)
    swapxy=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),os.path.basename(self.path))
        self.gtype='GEOMETRY'
    def pull(self):
        """Pulls the geojson data from the internet and unpacks it in the cache directory"""
        uri=http(self.url)
        basename=os.path.basename(self.path)
        uri.download(direc=self.cacheDir(),outfile=basename)


def NaturalEarthClassFactory(clsName,catentry):
    return type(clsName, (NaturalEarthBase,), {"url":catentry["url"],"path":catentry["path"]})

def getNaturalEarthDsets(conf):
    """retrieves the available natural earth datasets"""
    reponame="nvkelso/natural-earth-vector"
    commitsha="bf7720b54dd9ac2d4d7f735174901b3862b5362a"
    cachedir=conf.getCacheDir("githubcache")
    try:
        cred=conf.authCred("github",['oauthtoken'])
        token=cred.oauthtoken
    except:
        token=None
    # import pdb;pdb.set_trace() 
    catalog=cachedGithubCatalogue(reponame,cachedir=cachedir,commitsha=commitsha,
                       gfilter=ghfilter({"type":"blob","path":"\.geojson"}),
                       gfollowfilter=ghfilter({"type":"tree","path":"geojson"}),
                       ghtoken=token)

    out=[]
    #create a list of datasets
    for entry in catalog["datasets"]:
        clsname=os.path.basename(entry["path"]).split(".")[0]
        out.append(NaturalEarthClassFactory(clsname,entry))

    return out


geoslurpCatalogue.addDatasetFactory(getNaturalEarthDsets)



