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
# provides a dataset and table for static gravity fields from the icgem website



from geoslurp.dataset import DataSet,GravitySHTBase
from geoslurp.datapull.icgem import  Crawler as IcgemCrawler
import re
import gzip as gz
import logging
from glob import glob
from geoslurp.datapull import UriFile
import os
from datetime import datetime

def icgemMetaExtractor(uri):
    """Extract meta information from a gzipped icgem file"""

    #first extract the icgem header
    headstart=False
    hdr={}
    with gz.open(uri.url,'rt') as fid:
        logging.info("Extracting info from %s"%(uri.url))
        for ln in fid:
            # if "begin_of_head" in ln:
            #     headstart=True
            #     continue

            if headstart and 'end_of_head' in ln:
                break

            # if headstart:
            spl=ln.split()
            if len(spl) == 2:
                hdr[spl[0]]=spl[1]





    meta={"nmax":int(hdr["max_degree"]),
          "lastupdate":uri.lastmod,
          "format":"icgem",
          "gm":float(hdr["earth_gravity_constant"]),
          "re":float(hdr["radius"]),
          "uri":uri.url,
          "type":"GSM",
          "data":{"name":hdr["modelname"]}
          }

    #add tide system
    try:
        tmp=hdr["tide_system"]
        if re.search('zero_tide',tmp):
            meta["tidesystem"]="zero-tide"
        elif re.search('tide_free'):
            meta["tidesystem"]="tide-free"
    except:
        pass

    return meta


class ICGEM_static(DataSet):
    """Manages the static gravity fields which are hosted at http://icgem.gfz-potsdam.de/tom_longtime"""
    table=type("ICGEM_staticTable",(GravitySHTBase,), {})
    __version__=(0,0)
    def __init__(self, scheme):
        super().__init__(scheme)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)
        self.updated=[]
    def pull(self,pattern=None,list=False):
        """Pulls static gravity fields from the icgem website
        :param pattern: only download files whose name obeys this regular expression
        :param list (bool): only list available models"""
        self.updated=[]
        crwl=IcgemCrawler()
        if pattern:
            regex=re.compile(pattern)
        outdir=self.dataDir()
        if list:
            print("%12s %5s %4s"%("name","nmax", "year"))
        for uri in crwl.uris():
            if pattern:
                if not regex.search(uri.name):
                    continue
            if list:
                #only list available models
                print("%-12s %5d %4d"%(uri.name,uri.nmax,uri.lastmod.year))
            else:
                tmp,upd=uri.download(outdir,check=True, gzip=True)
                if upd:
                    self.updated.append(tmp)

    def register(self,pattern=None):

        #create a list of files which need to be (re)registered
        if self.updated:
            files=self.updated
        else:
            files=[UriFile(file) for file in glob(self.dataDir()+'/*.gz')]

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

            meta=icgemMetaExtractor(uri)
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

