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
from geoslurp.datapull.uri import findFiles
from geoslurp.config.slurplogger import slurplogger
from geoslurp.datapull import UriFile
from datetime import datetime
from geoslurp.tools.gravity import GravitySHTBase, icgemMetaExtractor
import re
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.db.settings import getCreateDir

scheme='gravity'

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
    scheme=scheme
    release=''
    subdirs=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.db.dbeng, checkfirst=True)
        if not self._dbinvent.datadir:
            self._dbinvent.datadir=getCreateDir(os.path.join(self.conf.getDataDir(self.scheme),self.release,self.subdirs,self.conf.mirrorMap))

    def pull(self):
        url=os.path.join("ftp://ftp.tugraz.at/outgoing/ITSG/GRACE/",self.release,self.subdirs)
        ftp=ftpCrawler(url,pattern='.*.gfc',followpattern='([0-9]{4})')

        self.updated=ftp.parallelDownload(self._dbinvent.datadir,check=True,gzip=True, maxconn=20)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in findFiles(self._dbinvent.datadir,'.*gfc.gz',since=self._dbinvent.lastupdate)]

        newfiles=self.retainnewUris(files)
        #loop over files
        for uri in newfiles:
            slurplogger().info("extracting meta info from %s"%(uri.url))
            meta=icgemMetaExtractor(uri)
            meta=enhanceMeta(meta)
            self.addEntry(meta)

        self.updateInvent()


def TUGRAZGRACEL2ClassFactory(release,subdirs):
    """Dynamically construct GRACE Level 2 dataset classes for TU GRAZ"""
    base,gtype=subdirs.split('/')
    clsName="_".join([release,gtype])
    table=type(clsName.replace('-',"_") +"Table", (GravitySHTBase,), {})
    return type(clsName, (TUGRAZGRACEL2Base,), {"release": release, "table":table,"subdirs":subdirs})

# setup GRACE datasets
def TUGRAZGRACEDsets(conf):
    out=[]
    release='ITSG-Grace2018'
    for subdirs in ["daily_kalman/daily_background","daily_kalman/daily_n40","monthly/monthly_background","monthly/monthly_n60","monthly/monthly_n96","monthly/monthly_n120"]:
        out.append(TUGRAZGRACEL2ClassFactory(release,subdirs))
    return out

geoslurpCatalogue.addDatasetFactory(TUGRAZGRACEDsets)
