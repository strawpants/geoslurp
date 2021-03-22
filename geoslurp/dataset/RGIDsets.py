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


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dataset.CSVBase import CSVBase
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from geoslurp.db.settings import getCreateDir
from sqlalchemy import Integer, String, Float
from zipfile import ZipFile
from datetime import datetime
import subprocess
import os
from glob import glob
import shutil
from numpy import arange
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.db.settings import getCreateDir


def csvLookup():
    """Returns a dictionary which maps columns names in the RGI csv files to sqlalchemy column types"""
    lookup={"Region":String,"O1":String, "O2":String,"Count":Integer,"Area":Float,"HypArea":Float,
            "RGIId":String,"GLIMSId":String,"Name":String,"CenLon":Float,"CenLat":Float,"FoGId":String,
            "PolUnit":String}
    for elev in arange(25,9000,50):
        lookup[str(elev)]=Float
    return lookup

def pullRGI(downloaddir,comparewithversion):
        httpserv=http('http://www.glims.org/RGI/rgi60_files/00_rgi60.zip',lastmod=datetime(2018,1,1))
        #Newest version which is supported by this plugin
        newestver=(6,0)
        upd=False
        #now determine whether to retrieve the file
        if newestver > comparewithversion:
            uri,upd=httpserv.download(downloaddir,check=True)
            if not os.path.exists(os.path.join(downloaddir,'extract')):
                #unzip all the goodies
                zipd=os.path.join(downloaddir,'zipfiles')
                with ZipFile(uri.url,'r') as zp:
                    zp.extractall(zipd)

                #now recursively zip the other zip files
                for zf in glob(zipd+'/*zip'):
                    slurplogger().info("Unzipping %s"%(zf))
                    with ZipFile(zf,'r') as zp:
                        zp.extractall(os.path.join(downloaddir,'extract'))
                #remove zipfiles after extracting
                shutil.rmtree(zipd)

                #Patches one csv file which contains a . instead of a , at one point
                #download patch from github
                pf='04_rgi60_ArcticCanadaSouth_hypso.csv.patch'
                slurplogger().info("Patching csv file %s"%(pf) )
                httpget=http("https://raw.githubusercontent.com/strawpants/geoslurp/master/patches/"+pf)
                uri,pupd=httpget.download(os.path.join(downloaddir,'extract'),check=True)
                #apply patch
                subprocess.Popen(['patch','-i',pf],cwd=os.path.dirname(uri.url))
        else:
            slurplogger().info("RGI 6.0 already downloaded")


        return newestver,upd

class RGIBase(OGRBase):
    """Base class for the shapefiles from the Randalph Glacier Inventory Datasets. They are all quite similar so letting them inherit from a baseclass
    seems reasonable
    """
    scheme='cryo'
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.setCacheDir(self.conf.getCacheDir(self.scheme,subdirs='RGI'))
        if "RGIversion"  in self._dbinvent.data:
            self._dbinvent.data["RGIversion"]=tuple(self._dbinvent.data["RGIversion"])
        else:
            self._dbinvent.data["RGIversion"] = (0, 0)

        self.ogrfile=os.path.join(self.cacheDir(),'extract',self.filename)

    def pull(self):
        """Pulls the entire RGI archive from the web and stores it in a cache"""
        version,updated=pullRGI(self.cacheDir(),self._dbinvent.data['RGIversion'])
        
        self._dbinvent.data["RGIversion"] = version
        self.updateInvent(False)

class RGICSVBase(CSVBase):
    """Base class for the CSV files from the Randalph Glacier Inventory Datasets. They are all quite similar so letting them inherit from a baseclass
    seems reasonable
    """
    scheme='cryo'
    lookup=csvLookup()
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.setCacheDir(self.conf.getCacheDir(self.scheme,subdirs='RGI'))
        if "RGIversion"  in self._dbinvent.data:
            self._dbinvent.data["RGIversion"]=tuple(self._dbinvent.data["RGIversion"])
        else:
            self._dbinvent.data["RGIversion"] = (0, 0)

        self.csvfile=os.path.join(self.cacheDir(),'extract',self.filename)

    def pull(self):
        """Pulls the entire RGI archive from the web and stores it in a cache"""
        version,updated=pullRGI(self.cacheDir(),self._dbinvent.data['RGIversion'])
        self._dbinvent.data["RGIversion"] = version
        self.updateInvent(False)


# Factory method to dynamically create classes
def RGISHPClassFactory(fileName):
    splt=fileName.split(".")
    return type(splt[0], (RGIBase,), {"filename":fileName,"gtype":"GEOMETRY"})

def RGICSVClassFactory(fileName,hskip=0):
    splt=fileName.split(".")
    return type(splt[0], (RGICSVBase,), {"filename":fileName,"hskip":hskip})


def getRGIDsets(conf):
    """Automatically create all classes contained within the RGI data"""
    out=[]
    regionnames=['01_rgi60_Alaska', '02_rgi60_WesternCanadaUS', '03_rgi60_ArcticCanadaNorth',
               '04_rgi60_ArcticCanadaSouth', '05_rgi60_GreenlandPeriphery', '06_rgi60_Iceland',
               '07_rgi60_Svalbard', '08_rgi60_Scandinavia', '09_rgi60_RussianArctic','10_rgi60_NorthAsia', '11_rgi60_CentralEurope',
               '12_rgi60_CaucasusMiddleEast', '13_rgi60_CentralAsia', '14_rgi60_SouthAsiaWest',
               '15_rgi60_SouthAsiaEast', '16_rgi60_LowLatitudes', '17_rgi60_SouthernAndes',
               '18_rgi60_NewZealand', '19_rgi60_AntarcticSubantarctic']

    
    out.append(RGISHPClassFactory("00_rgi60_O1Regions.shp"))
    out.append(RGISHPClassFactory("00_rgi60_O2Regions.shp"))
    out.append(RGICSVClassFactory("00_rgi60_summary.csv", 1))
    out.append(RGICSVClassFactory("00_rgi60_links.csv", 2))

    for nm in regionnames:
        out.append(RGISHPClassFactory(nm+".shp"))


    #also add important csv files
    for nm in regionnames:
        out.append(RGICSVClassFactory(nm+"_hypso.csv"))

    return out



geoslurpCatalogue.addDatasetFactory(getRGIDsets)
