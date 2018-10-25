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
from geoslurp.datapull import httpProvider as http
from geoslurp.config import Log
from geoslurp.meta import fillGeoTable
from zipfile import ZipFile
from datetime import datetime
import subprocess
import os

class RGIBase(DataSet):
    """Base class for Randalph Glacier Inventory Datasets. They are all quite similar so letting them inherit from a baseclass
    seems reasonable
    """
    __version__=(0,0,0)
    __csvlookup__=csvLookup()
    def __init__(self,scheme):
        super().__init__(scheme)
        if self._inventData:
            self._inventData["RGIversion"]=tuple(self._inventData["RGIversion"])
        else:
            self._inventData["RGIversion"] = (0, 0)

    def pull(self, force=False):
        """Pulls the entire RGI 6.0 archive from the web and stores it in a cache"""
            getf='00_rgi60.zip'
            httpserv=http('http://www.glims.org/RGI/rgi60_files/')

            #Newest version which is supported by this plugin
            newestver=(6,0)
            #now determine whether to retrieve the file
            if force or (newestver > self._inventData["RGIversion"]):
                fout=os.path.join(self.scheme.cache,getf)
                if os.path.exists(fout) and not force:
                    print (self.name+":File already in cache no need to download",file=Log)
                else:
                    httpserv.downloadFileByName(fout,getf)
                zipd=os.path.join(self.scheme.cache,'zipfiles')
                with ZipFile(fout,'r') as zp:
                    zp.extractall(zipd)

                #now recursively zip the other zip files
                for zf in glob(zipd+'/*zip'):
                    print("Unzipping %s"%zf,file=Log)
                    with ZipFile(zf,'r') as zp:
                        zp.extractall(os.path.join(self.scheme.cache,'extract'))
                self.patch()

                self._inventData["RGIversion"]=newestver
            else:
                print(self.name+": Already at newest version",file=Log)
                return

    def patch(self):
        """Patches one csv file which contains a . instead of a , at one point"""
        #download patch from github
        pf='04_rgi60_ArcticCanadaSouth_hypso.csv.patch'
        print("Patching csv file %s"%(pf), file=Log)
        httpget=http("https://raw.githubusercontent.com/strawpants/geoslurp/master/patches/"+pf)
        patchf=os.path.join(self.scheme.cache,'extract',pf)
        httpget.downloadFileByName(patchf)
        #apply patch
        subprocess.Popen(['patch','-i',pf],cwd=os.path.dirname(patchf))


    def register(self):
        """Register the (derived table)"""
        splt=self.name.split("_")
        folder=os.path.join(self.scheme.cache,self.base+"_shp",self.resolution)

        fillGeoTable(folder,self.name,self.scheme,regex=self.regex)

        #also update data entry from the inventory table
        self._inventData["lastupdate"]=datetime.now().isoformat(),
        self._inventData["version"]=self.__version__
        self.updateInvent()

    def purge(self):
        pass

# Factory method to dynamically create classes
def RGIClassFactory(clsName):
    splt=clsName.split("_")
    if len(splt) == 3:
        rgx=splt[1]
    else:
        rgx=None

    return type(clsName, (GSHHGBase,), {"resolution":splt[-1],"regex":rgx,"base":splt[0]})

def getRGIdict():
    """Automatically create all classes contained within the GSHHG database"""
    outdict={}
    overallregio
    regionnames=['01_rgi60_Alaska', '02_rgi60_WesternCanadaUS', '03_rgi60_ArcticCanadaNorth',
               '04_rgi60_ArcticCanadaSouth', '05_rgi60_GreenlandPeriphery', '06_rgi60_Iceland',
               '07_rgi60_Svalbard', '08_rgi60_Scandinavia', '09_rgi60_RussianArctic', '11_rgi60_CentralEurope',
               '12_rgi60_CaucasusMiddleEast', '13_rgi60_CentralAsia', '14_rgi60_SouthAsiaWest',
               '15_rgi60_SouthAsiaEast', '16_rgi60_LowLatitudes', '17_rgi60_SouthernAndes',
               '18_rgi60_NewZealand', '19_rgi60_AntarcticSubantarctic']

    #loop over all shapefiles
    for nm in ["00_rgi60_O1Regions","00_rgi60_O2Regions"]:
        outdict[nm]=RGIClassFactory(nm+".shp")
    for nm in regionnames:
        outdict[nm]=RGIClassFactory(nm+".shp")

    #also add important csv files


    return outdict

def csvLookup():
        for elev in arange(25,9000,50):
            sumlookup[str(elev)]='Float'
