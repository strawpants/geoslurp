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
from geoslurp.datapull.ftp import Crawler as ftp
from geoslurp.meta import fillGeoTable
from zipfile import ZipFile
from datetime import datetime
from geoslurp.config.slurplogger import slurplogger
import re
import os

class GSHHGBase(DataSet):
    """Base class for GSHHG datasets. They are all quite similar so letting them inherit from a baseclass
    seems reasonable
    """
    base=None
    resolution=None
    regex=None
    __version__=(0,0,0)
    def __init__(self,scheme):
        super().__init__(scheme)
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        if self._inventData:
            self._inventData["GSHHGversion"]=tuple(self._inventData["GSHHGversion"])
        else:
            self._inventData["GSHHGversion"] = (0, 0, 0)
            self._inventData["lastupdate"]=datetime.min.isoformat()

    def pull(self, force=False):
        """Pulls the entire GSHHG archive from the ftp server"""
        ftpdir=ftp('ftp://ftp.soest.hawaii.edu/gshhg/','gshhg-shp.*zip')
        #first find out the newest version
        vregex=re.compile('gshhg-shp-([0-9]\.[0-9]\.[0-9]).*zip')
        newestver=(0,0,0)

        #find out the newest version
        for uri in ftpdir.uris():
            match=vregex.findall(os.path.basename(uri.url))

            ver=tuple(int(x) for x in match[0].split('.'))
            if ver > newestver:
                newestver=ver
                geturi=uri

        #now determine whether to retrieve the file
        if force or newestver > self._inventData["GSHHGversion"]:
            furi=geturi.download(self.scheme.cache,True)

            with ZipFile(furi.url,'r') as zp:
                zp.extractall(self.scheme.cache)
            self._inventData["GSHHGversion"]=newestver

        else:
            slurplogger().info(self.name+": Already at newest version")
            return

    def register(self):
        """Register the (derived table)"""
        splt=self.name.split("_")
        folder=os.path.join(self.scheme.cache,self.base+"_shp",self.resolution)

        fillGeoTable(folder,self.name,self.scheme,regex=self.regex)

        #also update data entry from the inventory table
        self._inventData["lastupdate"]=datetime.now().isoformat(),
        self._inventData["version"]=self.__version__
        self.updateInvent()

    def halt(self):
        pass

    def purge(self):
        pass

# Factory method to dynamically create classes
def GSHHGClassFactory(clsName):
    splt=clsName.split("_")
    if len(splt) == 3:
        rgx=splt[1]
    else:
        rgx=None

    return type(clsName, (GSHHGBase,), {"resolution":splt[-1],"regex":rgx,"base":splt[0]})

def getGSHHGdict():
    """Automatically create all classes contained within the GSHHG database"""
    outdict={}
    for nm in ["GSHHS", "WDBII_river","WDBII_border"]:
        for res in ['c', 'l','i','h','f']:
            clsName=nm+"_"+res
            outdict[clsName]=GSHHGClassFactory(clsName)
    return outdict

