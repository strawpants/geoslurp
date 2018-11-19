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
from geoslurp.config import findFiles
import logging
import yaml
from geoslurp.datapull import UriFile
from io  import StringIO
import os
from datetime import datetime
from geoslurp.meta.gravity import GravitySHTBase, icgemMetaExtractor
import re

def enhanceMeta(meta):
    """Extract addtional timestamps from the TU graz filename data"""
    #from the TU GRAZ files one can extract the time period information
    yyyymm_match=re.match(".*([0-9]{4})-([0-9]{2})\.gfc\.gz$",meta["uri"])
    if yyyymm_match:
        yr=int(yyyymm_match.group(1))
        mn=int(yyyymm_match.group(2))
        meta["tstart"]=datetime(yr,mn,1)
        if mn ==12:
            meta['tend']=datetime(yr+1,1,1)
        else:
            meta["tend"]=datetime(yr,mn+1,1)
    else:
        yyyymmdd_match=re.match(".*([0-9]{4})-([0-9]{2})-([0-9]{2})\.gfc\.gz$",meta["uri"])
        if yyyymmdd_match:
            yr=int(yyyymmdd_match.group(1))
            mn=int(yyyymmdd_match.group(2))
            dd=int(yyyymmdd_match.group(3))
            meta['tstart']=datetime(yr,mn,dd)
            meta['tend']=datetime(yr,mn,dd,23,59)
    return meta

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
        ftp=ftpCrawler(url,pattern='.*.gfc',followpattern='(monthly_?)|(daily)|([0-9]{4})')
        self.updated=ftp.parallelDownload(self.dataDir(),check=True,gzip=True, maxconn=20)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in findFiles(self.dataDir(),'.*gfc.gz')]


        #loop over files
        for uri in files:
            urilike=os.path.basename(uri.url)

            if not self.uriNeedsUpdate(urilike,uri.lastmod):
                continue

            meta=icgemMetaExtractor(uri)
            meta=enhanceMeta(meta)
            self.addEntry(meta)

        self._inventData["lastupdate"]=datetime.now().isoformat()
        self._inventData["version"]=self.__version__
        self.updateInvent()

        self.ses.commit()




def TUGRAZGRACEL2ClassFactory(clsName):
    """Dynamically construct GRACE Level 2 dataset classes"""
    table=type(clsName.replace('-',"_") +"Table", (GravitySHTBase,), {})
    return type(clsName, (TUGRAZGRACEL2Base,), {"release": clsName, "table":table})

# setup GRACE datasets
def TUGRAZGRACEdict():
    outdict={}
    for release in ['ITSG-Grace2018']:
        outdict[release]=TUGRAZGRACEL2ClassFactory(release)
    return outdict
