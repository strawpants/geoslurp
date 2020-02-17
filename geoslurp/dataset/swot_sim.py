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

scheme='altim'

class SWOTSIMBase(OGRBase):
    """Base class for the swot simulation tracks (cal/val and science mode)"""
    scheme=scheme
    zipf=None
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #update ogrfile
        self.ogrfile=os.path.join(self.cacheDir(),self.ogrfile)

    def pull(self):
        """Pulls the shapefile from the aviso server"""
        httpserv=http(os.path.join('https://www.aviso.altimetry.fr/fileadmin/documents/missions/Swot/',self.zipf),lastmod=datetime(2019,9,6))
        cache=self.cacheDir() 
        uri,upd=httpserv.download(cache,check=True)

        if upd:
            with ZipFile(os.path.join(cache,self.zipf),'r') as zp:
                zp.extractall(cache)


# Factory method to dynamically create classes
def SWOTSIMClassFactory(ogrfile,zipfileName):
    splt=ogrfile.replace('-','_').replace('.','_').lower()[:-4]
    return type(splt, (SWOTSIMBase,), {"ogrfile":ogrfile,"zipf":zipfileName,"swapxy":True,"gtype":"GEOMETRY"})

def getSWOTSIMDsets(conf):
    """Automatically create all classes for the swot simulation tracks"""
    out=[]
    swotversions=[('swot_calval_orbit_june2015-v2_nadir.shp','shp_calval_nadir.zip'),
            ('swot_calval_orbit_june2015-v2_swath.shp','sph_calval_swath.zip'),
            ('swot_science_orbit_sept2015-v2_10s_nadir.shp','swot_science_orbit_sept2015-v2_10s_nadir.zip'),
            ('swot_science_orbit_sept2015-v2_10s_swath.shp','swot_science_orbit_sept2015-v2_10s_swath.zip'),
            ('swot_science_hr_2.0s_4.0s_June2019-v3_nadir.shp','swot_science_hr_2.0s_4.0s_June2019-v3_nadir.zip'),
            ('swot_science_hr_2.0s_4.0s_June2019-v3_swath.shp','swot_science_hr_2.0s_4.0s_June2019-v3_swath.zip')]
    for ogr,zipf in swotversions:
        out.append(SWOTSIMClassFactory(ogr,zipf))


    return out



geoslurpCatalogue.addDatasetFactory(getSWOTSIMDsets)
