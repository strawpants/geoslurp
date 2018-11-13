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
from geoslurp.datapull.webdav import Crawler as WbCrawler
import logging
from glob import glob
import gzip
import yaml
from geoslurp.datapull import UriFile
from io  import StringIO
import os
from datetime import datetime
from geoslurp.meta.gravity import GravitySHTBase

def graceMetaExtractor(uri):
    """Extract meta information from a GRACE file"""
    buf=StringIO()
    with gzip.open(uri.url,'rt') as fid:
        logging.info("Extracting info from %s"%(uri.url))
        for ln in fid:
            if '# End of YAML header' in ln:
                break
            else:
                buf.write(ln)
    hdr=yaml.safe_load(buf.getvalue())["header"]
    nonstand=hdr["non-standard_attributes"]


    meta={"nmax":hdr["dimensions"]["degree"],
          "omax":hdr["dimensions"]["order"],
          "tstart":hdr["global_attributes"]["time_coverage_start"],
          "tend":hdr["global_attributes"]["time_coverage_end"],
          "lastupdate":uri.lastmod,
          "format":nonstand["format_id"]["short_name"],
          "gm":nonstand["earth_gravity_param"]["value"],
          "re":nonstand["mean_equator_radius"]["value"],
          "uri":uri.url,
          "type":nonstand["product_id"][0:3],
          "data":{"description":hdr["global_attributes"]["title"]}
          }

    #add tide system
    try:
        tmp=nonstand["permanent_tide_flag"]
        if re.search('inclusive',tmp):
            meta["tidesystem"]="zero-tide"
        elif re.search('exclusive'):
            meta["tidesystem"]="tide-free"
    except:
        pass

    return meta

class GRACEL2Base(DataSet):
    """Derived type representing GRACE spherical harmonic coefficients on the podaac server"""
    release=None
    center=None
    updated=None
    __version__=(0,0)
    def __init__(self,scheme):
        super().__init__(scheme)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self):
        cred=self.scheme.conf.authCred("podaac")
        url="https://podaac-tools.jpl.nasa.gov/drive/files/allData/grace/L2/"+self.center+"/"+self.release
        webdav=WbCrawler(url,auth=cred,pattern='G.*gz')
        self.updated=webdav.parallelDownload(self.dataDir(),check=True)

    def register(self):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in glob(self.dataDir()+'/G*gz')]

        ses=self.scheme.db.Session()
        i=0
        #loop over files
        for uri in files:
            try:
                base=os.path.basename(uri.url)
                qResult=ses.query(self.table).filter(self.table.uri.like('%'+base+'%')).first()
                if qResult.lastupdate >= uri.lastmod:
                    logging.info("No Update needed, skipping %s"%(base))
                    continue
                else:
                    #delete the entries which need updating
                    ses.delete(qResult)
                    ses.commit()
            except Exception as e:
                # Fine no entries found
               pass

            meta=graceMetaExtractor(uri)
            try:
                entry=self.table(**meta)
                ses.add(entry)

                if i > 10:
                    # commit every so many rows
                    ses.commit()
                    i=0
                else:
                    i+=1
            except Exception as e:
                pass
        self._inventData["lastupdate"]=datetime.now().isoformat()
        self._inventData["version"]=self.__version__
        self.updateInvent()

        ses.commit()



    def halt(self):
        pass

    def purge(self):
        pass

def GRACEL2ClassFactory(clsName):
    """Dynamically construct GRACE Level 2 dataset classes"""
    base,center,release=clsName.split("_")
    table=type(clsName +"Table", (GravitySHTBase,), {})
    return type(clsName, (GRACEL2Base,), {"release": release, "center":center,"table":table})

# setup GRACE datasets
def GRACEdict():
    outdict={}
    for release in ["RL06"]:
        for center in ["CSR", "GFZ", "JPL"]:
            clsName="GRACEL2"+"_"+center+"_"+release
            outdict[clsName]=GRACEL2ClassFactory(clsName)
    return outdict
