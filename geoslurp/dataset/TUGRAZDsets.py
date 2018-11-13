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

from geoslurp.dataset import DataSet
from geoslurp.datapull.ftp import Crawler as ftpCrawler
import logging
from glob import glob
import gzip
import yaml
from geoslurp.datapull import UriFile
from io  import StringIO
import os
from datetime import datetime
from geoslurp.meta.gravity import GravitySHTBase, icgemMetaExtractor


class TUGRAZGRACEL2Base(DataSet):
    """Derived type representing GRACE spherical harmonic coefficients from the TU GRAZ"""
    table=None
    updated=None
    __version__=(0,0)
    def __init__(self,scheme):
        super().__init__(scheme)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self):
        url="ftp://ftp.tugraz.at/outgoing/ITSG/GRACE/"+self.__class__.__name__
        ftp=ftpCrawler(url,pattern='.*gfc',followpattern='monthly_.*')
        self.updated=ftp.parallelDownload(self.dataDir(),check=True)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in glob(self.dataDir()+'/*.gfc.gz')]

        #loop over files
        for uri in files:
            urilike=os.path.basename(uri.url)

            if not self.uriNeedsUpdate(urilike,uri.lastmod):
                continue

            meta=icgemMetaExtractor(uri)

            self.addEntry(meta)

        self._inventData["lastupdate"]=datetime.now().isoformat()
        self._inventData["version"]=self.__version__
        self.updateInvent()

        self.ses.commit()



    def halt(self):
        pass

    def purge(self):
        pass

def TUGRAZGRACEL2ClassFactory(clsName):
    """Dynamically construct GRACE Level 2 dataset classes"""
    table=type(clsName +"Table", (GravitySHTBase,), {})
    return type(clsName, (TUGRAZGRACEL2Base,), {"release": clsName, "table":table})

# setup GRACE datasets
def TUGRAZGRACEdict():
    outdict={}
    for release in ['ITSG-Grace2018']:
        outdict[release]=TUGRAZGRACEL2ClassFactory(release)
    return outdict
