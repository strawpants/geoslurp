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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from zipfile import ZipFile
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from datetime import datetime
from sqlalchemy import Column, Integer
import re

scheme='altim'

class AvisoRefOrbitBase(OGRBase):
    """Base class for Aviso reference orbit tracks"""
    scheme=scheme
    fromurl=""
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #update ogrfile
        self.ogrfile=os.path.join(self.cacheDir(),self.ogrfile)
    
    
    def valuesFromOgrFeat(self,feat,transform=None):
        """Extracts and adds the pass number from the feature"""
        vals=super().valuesFromOgrFeat(feat,transform)
        passmatch=re.search('(?:Ground Track|Pass) ([0-9]+)',vals['name'])
#         import pdb;pdb.set_trace()
        if passmatch:
            vals["pass"]=int(passmatch.group(1))
        return vals    
        


    def columnsFromOgrFeat(self,feat):
        cols=super().columnsFromOgrFeat(feat)
        cols.append(Column('pass', Integer))
        return cols
    
    def pull(self):
        """Pulls the shapefile from the aviso server"""
        httpserv=http(self.fromurl,lastmod=datetime(2019,9,6))
        cache=self.cacheDir() 
        uri,upd=httpserv.download(cache,check=True)
        
#         if upd:
#             with ZipFile(os.path.join(cache,self.zipf),'r') as zp:
#                 zp.extractall(cache)


# Factory method to dynamically create classes
def AvisoClassFactory(kmzurl):
    splt=kmzurl.split("/")
#     splt=kmzurl.split("/")[-1].replace('-','_').replace('.','_').lower()[:-4]
    clsName=splt[-1].replace('-','_').replace('.','_').lower()[:-4]
    return type(clsName, (AvisoRefOrbitBase,), {"ogrfile":splt[-1],"fromurl":kmzurl,"swapxy":True,"gtype":"GEOMETRYZ","ignoreFields":"(timestamp)|(begin)|(end)|(altitude)|(tessellate)|(extrude)|(visibility)|(drawOrder)|(icon)|(snippet)"})

def getAvisoRefOrbits(conf):
    """Automatically create all classes for the Aviso reference orbits"""
    out=[]
    rooturl='https://www.aviso.altimetry.fr/fileadmin/documents/missions/Swot/'
    swotversions=['swot_science_orbit_sept2015-v2_10s.kmz','swot_science_orbit_sept2015-v2_perDay.kml','swot_science_hr_2.0s_4.0s_June2019-v3_perPass.kml']
    for kmzf in swotversions:
        out.append(AvisoClassFactory(rooturl+kmzf))

    rooturl2="https://www.aviso.altimetry.fr/fileadmin/documents/data/tools/"
    altversions=['Visu_RefOrbit_J3J2J1TP_Tracks_GoogleEarth_V3.kmz','Visu_J1TP_Interlaced_Tracks_GE_V3.kmz','swot_science_hr_2.0s_4.0s_June2019-v3_perPass.kml',
                'Visu_J2_LRO_Cycle500_537.kmz','Visu_EN_Tracks_GE_OldOrbit.kmz','Visu_ENN_Tracks_GE_NewOrbit.kmz','Visu_C2_Tracks_HiRes.kmz',
                'Visu_G2_Tracks_GE_V3.kmz']
    for kmzf in altversions:
        out.append(AvisoClassFactory(rooturl2+kmzf))
     
    return out





geoslurpCatalogue.addDatasetFactory(getAvisoRefOrbits)
