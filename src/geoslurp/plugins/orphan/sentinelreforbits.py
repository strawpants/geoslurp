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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2021


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from zipfile import ZipFile
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
from sqlalchemy import Column, Integer,ARRAY,String
# from sqlalchemy.dialects.postgresql import TIMESTAMP
import re
# from sqlalchemy.ext.declarative import declared_attr, as_declarative
# from sqlalchemy import MetaData
# from math import modf



scheme='altim'


class S3ABRefOrbitsBase(OGRBase):
    """Base class for Sentinel 3A/B reference orbit tracks"""
    scheme=scheme
    gtype="GEOMETRYZ"
    swapxy=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
        #also set the cachedirectory to a common directory 
        self.setCacheDir(self.conf.getCacheDir(self.scheme, dataset="Sentinel3orbit"))
    
        #update ogrfile
        self.ogrfile=os.path.join(self.cacheDir(),self.ogrfile)

    def valuesFromOgrFeat(self,feat,transform=None):
        """Extracts and adds the relative orbit number from the feature"""
        vals=super().valuesFromOgrFeat(feat,transform)
        orbitmatch=re.search('(?:RELATIVE ORBIT) ([0-9]+)',vals['name'])
        if orbitmatch:
            vals["orbit"]=int(orbitmatch.group(1))
        vals["missionids"]=self.missionids
        return vals    
        


    def columnsFromOgrFeat(self,feat):
        cols=super().columnsFromOgrFeat(feat)
        cols.append(Column('orbit', Integer))
        cols.append(Column('missionids', ARRAY(String)))
        return cols
    
    def pull(self):
        """Pulls the google kml files from the copernicus server"""
        rooturl='https://sentinel.esa.int/documents/247904/685098/Sentinel-3-Absolute-Ground-Tracks.zip'
        cache=self.cacheDir() 
        httpserv=http(rooturl,lastmod=datetime(2021,11,29))
        uri,upd=httpserv.download(cache,check=True)
        
        if upd:
            with ZipFile(uri.url,'r') as zp:
                zp.extractall(cache)

        
class s3a_reforbit(S3ABRefOrbitsBase):
    ogrfile='S3A_rel_orbit_ground_track_10sec_v1_4.kml'
    missionids=["s3a"]

class s3b_reforbit(S3ABRefOrbitsBase):
    ogrfile='S3B_rel_orbit_ground_track_10sec_v1_4.kml'
    missionids=["s3b"]

geoslurpCatalogue.addDataset(s3a_reforbit)
geoslurpCatalogue.addDataset(s3b_reforbit)
