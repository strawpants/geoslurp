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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2025


from geoslurp.dataset.OGRBase import OGRBase
from geoslurp.dbfunc.dbfunc import DBFunc
from geoslurp.datapull.http import Uri as http
from geoslurp.config.slurplogger import slurplogger
from zipfile import ZipFile
from datetime import datetime
import os

schema="hydrosheds"
class HydroshedBase(OGRBase):
    """Base class for the shapefiles/geodatabase from the hydrosheds family"""
    schema=schema
    hytype=''
    filename=''
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.setCacheDir(self.conf.getCacheDir(self.schema,subdirs=self.hytype))
        self.ogrfile=os.path.join(self.cacheDir(),'extract',self.filename)

    def pull(self):
        """Pulls the relevant geodatabase and stores it in a cache"""
        # https://data.hydrosheds.org/file/HydroBASINS/standard/hybas_af_lev06_v1c.zip
        url=f"https://data.hydrosheds.org/file/hydrobasins/standard/{self.name}_v1c.zip"
        downloaddir=self.cacheDir()
        httpserv=http(url,lastmod=datetime(2021,2,8))
        #Newest version which is supported by this plugin
        uri,upd=httpserv.download(downloaddir,check=True)
        if upd:
            #unzip all the goodies
            zipd=os.path.join(downloaddir,'extract')
            with ZipFile(uri.url,'r') as zp:
                zp.extractall(zipd)
        else:
            slurplogger().info("This component of hydrosheds is already downloaded")


# Factory method to dynamically create classes for the hydrosheds
def hydrobasinsClassFactory(hytype,lev):
    name="%s_lev%02d"%(hytype,lev)
    fname="%s_v1c.shp"%(name)
    return type(name, (HydroshedBase,), {"hytype":hytype,"filename":fname,"gtype":"GEOMETRY","swapxy":True})

# def hydroriverClassFactory(hytype):
    # fname="%s.shp"%(hytype)
    # return type(hytype, (HydroshedBase,), {"hytype":hytype,"filename":fname,"gtype":"GEOMETRY","swapxy":True})

# def getHyRivers(conf):
    # out=[]
    # for hytype in ["af_riv_30s","eu_riv_30s"]:
        # out.append(hydroriverClassFactory(hytype))
    # return out

def getHyBasins(conf):
    out=[]
    for hytype in ["hybas_af","hybas_eu","hybas_ar","hybas_as","hybas_au","hybas_na","hybas_sa","hybas_si","hybas_gr"]:
        for lev in range(1,13):
            out.append(hydrobasinsClassFactory(hytype,lev))

    return out



class max_upstream_pfaf_id(DBFunc):
    """Registers a plpython function which can read a file from the server and return a bytestring"""
    language="plpython3u"
    inargs="pfafid bigint, nignore integer"
    outargs="bigint"
    pgbody="""
            import re
            nign=nignore
            if nign is None:    
                nign=1
            match=re.search(r'[2468]{1}',str(pfafid)[:2:-1][nign:])
            if match is None:
               exp=10**9
            else:
               exp=10**(match.start()+nign)
            return int(pfafid/exp+1)*exp
            """
