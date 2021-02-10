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

def pullHydro(hytype,downloaddir):
        # see https://www.dropbox.com/sh/hmpwobbz9qixxpe/AAAI_jasMJPZl_6wX6d3vEOla for the 'root' of the hydrsheds data 
        hysource={"hybas_af":
                "https://www.dropbox.com/sh/hmpwobbz9qixxpe/AADoPLdVZNd2JG-KaJNY0zT1a/HydroBASINS/standard/af/hybas_af_lev01-06_v1c.zip",
                "hybas_eu":
                "https://www.dropbox.com/sh/hmpwobbz9qixxpe/AABz1Pym5esD6GUJcnzaaqpEa/HydroBASINS/standard/eu/hybas_eu_lev01-06_v1c.zip",
                "af_riv_30s":
                "https://www.dropbox.com/sh/hmpwobbz9qixxpe/AAC9imuUajl_1bS0tKWqPE8Ya/HydroSHEDS_RIV/RIV_30s/af_riv_30s.zip",
                "eu_riv_30s":
                "https://www.dropbox.com/sh/hmpwobbz9qixxpe/AAD68vqkhRNJd5qK3NVvM7TSa/HydroSHEDS_RIV/RIV_30s/eu_riv_30s.zip"}
        httpserv=http(hysource[hytype],lastmod=datetime(2021,2,8))
        #Newest version which is supported by this plugin
        uri,upd=httpserv.download(downloaddir,check=True)
        if upd:
            #unzip all the goodies
            zipd=os.path.join(downloaddir,'extract')
            with ZipFile(uri.url,'r') as zp:
                zp.extractall(zipd)
        else:
            slurplogger().info("This component of hydrosheds is already downloaded")


        return upd


class HydroshedBase(OGRBase):
    """Base class for the shapefiles/geodatabase from the hydrosheds family"""
    scheme='hydrosheds'
    hytype=''
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.setCacheDir(self.conf.getCacheDir(self.scheme,subdirs=self.hytype))
        self.ogrfile=os.path.join(self.cacheDir(),'extract',self.filename)

    def pull(self):
        """Pulls the relevant geodatabase and stores it in a cache"""
        upd=pullHydro(self.hytype,self.cacheDir())



# Factory method to dynamically create classes for the hydrosheds
def hydrobasinsClassFactory(hytype,lev):
    name="%s_lev%02d"%(hytype,lev)
    fname="%s_v1c.shp"%(name)
    return type(name, (HydroshedBase,), {"hytype":hytype,"filename":fname,"gtype":"GEOMETRY","swapxy":True})

def hydroriverClassFactory(hytype):
    fname="%s.shp"%(hytype)
    return type(hytype, (HydroshedBase,), {"hytype":hytype,"filename":fname,"gtype":"GEOMETRY","swapxy":True})

def getHyRivers(conf):
    out=[]
    for hytype in ["af_riv_30s","eu_riv_30s"]:
        out.append(hydroriverClassFactory(hytype))
    return out

def getHyBasins(conf):
    out=[]
    for hytype in ["hybas_af","hybas_eu"]:
        for lev in range(1,7):
            out.append(hydrobasinsClassFactory(hytype,lev))

    return out

geoslurpCatalogue.addDatasetFactory(getHyRivers)
geoslurpCatalogue.addDatasetFactory(getHyBasins)


