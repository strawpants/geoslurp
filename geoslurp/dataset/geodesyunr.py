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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018
from geoslurp.dataset import DataSet
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from sqlalchemy import Column,Integer,String, Boolean,Float,ARRAY
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from geoslurp.datapull.geodesyunr import Crawler as UnrCrawler
from geoalchemy2.types import Geography
# from geoalchemy2.elements import WKBElement
from datetime import datetime,timedelta
import os
from osgeo import ogr
from geoslurp.config.slurplogger import slurplogger
from geoslurp.config.catalogue import geoslurpCatalogue

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

scheme='gnss'
GNSSTBase=declarative_base(metadata=MetaData(schema=scheme))

class UNRTable(GNSSTBase):
    __tablename__='unrfinal'
    id=Column(Integer,primary_key=True)
    statname=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    nsolu=Column(Integer)
    xyz=Column(ARRAY(Float))
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    uri=Column(String, index=True)
    geom=Column(geoPointtype)
    data=Column(JSONB)


def enhancetenv3Meta(meta,file):
    """Reorder some entries so that they agree with The only thing whch is currently indexed is that the uri is replaced by the filename,but more adavanced meta data extractions may be adaded here"""
    meta["uri"]=file
    meta["data"]={}

    # adapt later to extract additional info
    # with gz.open(file,'rt') as fid:
    #     slurplogger().info("Extracting info from %s"%(file))
    #     for cnt,ln in enumerate(fid):
    #         pass

    #extract geographic point

    geoLoc=ogr.Geometry(ogr.wkbPoint)
    geoLoc.AddPoint(meta.pop("lon"),meta.pop("lat"),meta.pop("height"))
    meta["geom"]=geoLoc.ExportToWkt()
    return meta


class UNRfinal(DataSet):
    """Base class to store RLR/MET annual and monthly data"""
    table=UNRTable
    updated=None
    scheme=scheme
    def __init__(self,dbconn):
        super().__init__(dbconn)
        GNSSTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self):
        crwl=UnrCrawler(catalogfile=os.path.join(self.dataDir(),'DataHoldings.txt'))
        self.updated=crwl.parallelDownload(self.dataDir(),check=True,gzip=True,maxconn=10)


    def register(self):
        #create a list of files which need to be (re)registered

        crwl=UnrCrawler(catalogfile=os.path.join(self.dataDir(),'DataHoldings.txt'))

        for uri in crwl.uris(refresh=False):

            if not self.uriNeedsUpdate(uri["statname"],uri["lastupdate"]):
                continue

            localfile=os.path.join(self.dataDir(),os.path.basename(uri["uri"]+".gz"))
            if not os.path.exists(localfile):
                slurplogger().info("skipping %s"%(localfile))
                continue
            slurplogger().info("Registering %s"%(localfile))
            meta=uri.dict
            meta=enhancetenv3Meta(meta,localfile)
            self.addEntry(meta)

        self._dbinvent.data["citation"]="Blewitt, G., W. C. Hammond, and C. Kreemer (2018), " \
                                     "Harnessing the GPS data explosion for interdisciplinary science, Eos, 99, https://doi.org/10.1029/2018EO104623."
        self.updateInvent()

geoslurpCatalogue.addDataset(UNRfinal)


