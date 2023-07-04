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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from geoslurp.dataset import DataSet
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String
from geoalchemy2.types import Geography
from osgeo import ogr
from geoalchemy2.elements import WKBElement
from geoslurp.datapull.http import Uri as http
from datetime import datetime
from zipfile import ZipFile
from geoslurp.config.slurplogger import slurplogger
from geoslurp.datapull import UriFile
from geoslurp.datapull import findFiles
from geoslurp.config.catalogue import geoslurpCatalogue
import re
import os

scheme="oceanobs"
FrontsTBase=declarative_base(metadata=MetaData(schema=scheme))


geoLineStrType = Geography(geometry_type="MULTILINESTRING", srid='4326', spatial_index=True,dimension=2)

class OrsifrontsTable(FrontsTBase):
    """Defines the Orsifonts PostgreSQL table"""
    __tablename__='orsifronts'
    id=Column(Integer,primary_key=True)
    name=Column(String)
    acronym=Column(String)
    geom=Column(geoLineStrType)


def orsiMetaExtractor(uri):
    """extract table data from the files"""
    lookup={"stf":"Subtropical front", "saf":"Subantarctic front", "pf":"Polar front","saccf":"Southern Antarctic circumpolar current front", "sbdy":"Southern Boundary of the Antarctic circumpolar current"}
    abbr=os.path.basename(uri.url)[:-4]
    geofront=ogr.Geometry(ogr.wkbMultiLineString)
    frontsegment=ogr.Geometry(ogr.wkbLineString)
    recomm=re.compile("^%")
    with open(uri.url,'r') as fid:
        for ln in fid:
            if recomm.search(ln):
                #everytime we encounter a % we need to start a new segment and possibly update the multilinestring
                if frontsegment.GetPointCount() > 1:
                    geofront.AddGeometry(frontsegment)
                frontsegment=ogr.Geometry(ogr.wkbLineString)
                continue
            lonlat=ln.split()
            frontsegment.AddPoint(float(lonlat[0]),float(lonlat[1]))
    geofront.FlattenTo2D()
    meta={"acronym":abbr,"name":lookup[abbr],"geom":WKBElement(geofront.ExportToIsoWkb(),srid=4326)}
    return meta


class Orsifronts(DataSet):
    """Orsifronts table"""
    version=(0,0,0)
    table=OrsifrontsTable
    scheme=scheme
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.table.metadata.create_all(self.db.dbeng, checkfirst=True)
        self._dbinvent.data={"citation":"Orsi, A. H., T. Whitworth III and W. D. Nowlin, Jr. (1995). On the meridional extent and fronts of the Antarctic Circumpolar Current, Deep-Sea Research I, 42, 641-67"}

    def pull(self):
        """Download acsii files in zip and unpack ascii data"""
        httpserv=http('https://github.com/AustralianAntarcticDivision/orsifronts/raw/master/data-raw/fronts.zip',lastmod=datetime(2018,1,1))
        uri,upd=httpserv.download(self.cacheDir(),check=True)
        if upd:
            #unpack zip
            with ZipFile(uri.url,'r') as zp:
                zp.extractall(self.cacheDir())



    def register(self):
        """ Register all downloaded fronts (in text files)"""

        slurplogger().info("Building file list..")
        files=[UriFile(file) for file in findFiles(self.cacheDir(),'.*txt',self._dbinvent.lastupdate)]

        if len(files) == 0:
            slurplogger().info("Orsifronts: No new files found since last update")
            return

        #possibly empty table
        self.truncateTable()

        #loop over files
        for uri in files:
            slurplogger().info("adding %s"%(uri.url))
            self.addEntry(orsiMetaExtractor(uri))


        self.updateInvent()



#register dataset
geoslurpCatalogue.addDataset(Orsifronts)
