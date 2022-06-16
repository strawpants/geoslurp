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
# provides a dataset and table for static gravity fields from the icgem website



from geoslurp.dataset import DataSet
from geoslurp.tools.gravity import GravitySHTBase
from geoslurp.datapull.icgem import  Crawler as IcgemCrawler
from geoslurp.datapull.uri import findFiles
import re
import gzip as gz
from geoslurp.config.slurplogger import slurplogger
from glob import glob
from geoslurp.datapull import UriFile
import os
from datetime import datetime
from geoslurp.tools.gravity import icgemMetaExtractor
from geoslurp.config.catalogue import geoslurpCatalogue

class ICGEM_static(DataSet):
    """Manages the static gravity fields which are hosted at http://icgem.gfz-potsdam.de/tom_longtime"""
    table=type("ICGEM_staticTable",(GravitySHTBase,), {})
    scheme='gravity'
    stripuri=True
    def __init__(self, dbconn):
        super().__init__(dbconn)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.db.dbeng, checkfirst=True)
        self.updated=[]
    
    def pull(self,pattern=None,list=False):
        """Pulls static gravity fields from the icgem website
        :param pattern: only download files whose name obeys this regular expression
        :param list (bool): only list available models"""
        self.updated=[]
        crwl=IcgemCrawler()
        if pattern:
            regex=re.compile(pattern)
        outdir=self.dataDir()
        if list:
            print("%12s %5s %4s"%("name","nmax", "year"))
        for uri in crwl.uris():
            if pattern:
                if not regex.search(uri.name):
                    continue
            if list:
                #only list available models
                print("%-12s %5d %4d"%(uri.name,uri.nmax,uri.lastmod.year))
            else:
                tmp,upd=uri.download(outdir,check=True, gzip=True)
                if upd:
                    self.updated.append(tmp)

    def register(self,pattern=None):
        """Register static gravity fields donwloaded in the data director
        :param pattern: only register files whose filename obeys this regular expression
        """
        if not pattern:
            pattern='.*\.gz'
        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in findFiles(self.dataDir(),pattern)]

        #loop over files
        for uri in files:
            urilike=os.path.basename(uri.url)

            if not self.uriNeedsUpdate(urilike,uri.lastmod):
                continue

            meta=icgemMetaExtractor(uri)
            self.addEntry(meta)

        self.updateInvent()


geoslurpCatalogue.addDataset(ICGEM_static)

