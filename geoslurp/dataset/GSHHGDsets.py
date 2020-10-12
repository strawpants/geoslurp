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
from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.dataset.OGRBase import OGRBase
from zipfile import ZipFile
from datetime import datetime,timedelta
from geoslurp.config.slurplogger import slurplogger
import re
import os
from geoslurp.config.catalogue import geoslurpCatalogue

class GSHHGBase(OGRBase):
    """Base class for GSHHG datasets. They are all quite similar so letting them inherit from a baseclass
    seems reasonable
    """
    version=(0,0,0)
    scheme='globalgis'
    gshhgversion=(2,3,7)
    updatefreq=365
    swapxy=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.cache=self.conf.getCacheDir(self.scheme,subdirs="GSHHG")
        self.ogrfile=os.path.join(self.cache,self.path)
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        if self._dbinvent.data:
            self._dbinvent.data["GSHHGversion"]=tuple(self._dbinvent.data["GSHHGversion"])
        else:
            self._dbinvent.data["GSHHGversion"] = self.gshhgversion


    def pull(self):
        """Pulls the entire GSHHG archive from the ftp server"""
        url='ftp://ftp.soest.hawaii.edu/gshhg/gshhg-shp-%d.%d.%d.zip'%self.gshhgversion
        geturi=ftp(url,lastmod=datetime(2017,6,15))

        furi,upd=geturi.download(self.cache,True)
        if upd:
            with ZipFile(furi.url,'r') as zp:
                zp.extractall(self.cache)
            self.updateInvent(False)

# Factory method to dynamically create classes
def GSHHGClassFactory(clsName):
    splt=clsName.split("_")
    if len(splt) == 3:
        rgx=splt[1]
    else:
        rgx=None
    path=splt[0]+"_shp/"+splt[-1]
    return type(clsName, (GSHHGBase,), {"path":path,"layerregex":rgx})

def getGSHHGDsets(conf):
    """Automatically create all classes contained within the GSHHG database"""
    out=[]
    for nm in ["GSHHS", "WDBII_river","WDBII_border"]:
        for res in ['c', 'l','i','h','f']:
            clsName=nm+"_"+res
            out.append(GSHHGClassFactory(clsName))
    return out


geoslurpCatalogue.addDatasetFactory(getGSHHGDsets)
