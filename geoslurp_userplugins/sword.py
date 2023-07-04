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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2023
# Author Amin Shakya (a.shakya@utwente.nl), 2023

from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
import json
from zipfile import ZipFile
import os
import tempfile
import yaml

class SwordBase(OGRBase):
    """
    Base class for SWORD: the SWOT River Database, Version 15. If you would like more information about SWORD, please see the paper published in Water Resources Research in 2021, led by Elizabeth Altenau. You can access it here: https://agupubs.onlinelibrary.wiley.com/doi/10.1029/2021WR030054
    """
    url='http://gaia.geosci.unc.edu/SWORD/SWORD_v15_gpkg.zip'
    path="SWORD_v15_gpkg.zip"
    scheme='globalgis'
    version=(0,0,0)
    cont=None
    swordtype=None
    
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.ogrfile=os.path.join(self.cacheDir(),os.path.basename(self.path))
        self.gtype='GEOMETRY'
        self.setCacheDir(os.path.join(self.conf.getCacheDir(scheme=self.scheme),'SwordBase'))
        self.ogrfile=self.getogrfile()
        
        
    def pull(self):
        """Pulls the geojson data from the url and unpacks it in the cache directory"""
        uri=http(self.url,lastmod=datetime(2023,6,28))
        uri.download(direc=self.cacheDir(),outfile=self.path,check=True)               
        path_to_zip_file = os.path.join(self.cacheDir(), self.path)
        with ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(self.cacheDir())
        
    def getogrfile(self):
        return os.path.join(self.cacheDir("gpkg"),self.cont+"_sword_"+self.swordtype+"_v15.gpkg")

def SwordClassFactory(clsName,val):
    return type(clsName, (SwordBase,), {"cont":val["cont"], "swordtype":val["swordtype"]})

def getSwordDsets(conf):
    # Function does not initiate class instances, but only defines class types
    continents=["eu","af","sa","oc","as"]
    swordtypes = ["reaches", "nodes"]
    out=[]
    
    #create a list of datasets
    for continent in continents:
        for swordtype in swordtypes:
            entry={"cont":continent,"swordtype":swordtype}
            clsname=continent + "_sword_" + swordtype
            out.append(SwordClassFactory(clsname,entry))


    return out


geoslurpCatalogue.addDatasetFactory(getSwordDsets)
