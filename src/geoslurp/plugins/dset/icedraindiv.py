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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020


from geoslurp.dataset import DataSet
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import as_declarative,declared_attr
from sqlalchemy import Column,Float,Integer
from geoalchemy2.types import Geography
from geoalchemy2.elements import WKBElement
from geoslurp.datapull.http import Uri as http
from datetime import datetime
from geoslurp.config.slurplogger import slurplogger
from osgeo import ogr
import gzip as gz
import os
import re

schema='cryo'


geoPolyType = Geography(geometry_type="POLYGON", srid='4326', spatial_index=True,dimension=2)

@as_declarative(metadata=MetaData(schema=schema))
class draindivTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id = Column(Integer, primary_key=True)
    basinid=Column(Float)
    geom=Column(geoPolyType)

def IceSatPolygons(fname):
    """Generatori (co-routine) which reads polygons from a text file"""

    if not os.path.exists(fname):
        raise FileNotFoundError("Drainage divide file  %s not found is the data pulled?"%fname)

    if re.search("ant_.+polygons.txt",fname):
        idx=[2,1,0]
    else:
        idx=[0,2,1]

    with gz.open(fname,'rt') as fid:
        for ln in fid:
            if re.search("END OF HEADER",ln):
                break
        oldid=-1
        ring=None
        #read polygons
        for ln in fid:
            row=[float(x) for x in ln.split()]
            row=[row[i] for i in idx]
            if oldid == row[0]:
                ring.AddPoint(row[1],row[2])
            else:
                if ring:
                    #possibly return the last completed polygon
                    poly=ogr.Geometry(ogr.wkbPolygon)
                    poly.AddGeometry(ring)
                    poly.FlattenTo2D()
                    poly.CloseRings()
                    meta={"basinid":row[0],"geom":WKBElement(poly.ExportToIsoWkb(),srid=4326)}
                    yield meta
                oldid=row[0]
                #initialize a ring
                ring=ogr.Geometry(ogr.wkbLinearRing)
                ring.AddPoint(row[1],row[2])
        #return the last polygon
        poly=ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        poly.FlattenTo2D()
        poly.CloseRings()
        meta={"basinid":row[0],"geom":WKBElement(poly.ExportToIsoWkb(),srid=4326)}
        
        yield meta

class IceSatDDivBase(DataSet):
    """Icesat Drainage divide table base"""
    version=(0,0,0)
    schema=schema
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.table.metadata.create_all(self.db.dbeng, checkfirst=True)
        self._dbinvent.data={"citation":"Zwally, H. Jay, Mario B. Giovinetto, Matthew A. Beckley, and Jack L. Saba, 2012, Antarctic and Greenland Drainage Systems, GSFC Cryospheric Sciences Laboratory, at https://earth.gsfc.nasa.gov/cryo/data/polar-altimetry/antarctic-and-greenland-drainage-systems"}
        self.setCacheDir(self.conf.getCacheDir(self.schema,'Icesat_Draindiv'))

    def pull(self):
        """Download ascii file"""
        rooturl="https://earth.gsfc.nasa.gov/sites/default/files/lab_cryo/data/polar_ice_altimetry/antarctic_and_greenland_drainage_systems/"
        httpserv=http(os.path.join(rooturl,self.fbase),lastmod=datetime(2020,1,10))
        uri,upd=httpserv.download(self.cacheDir(),check=True,gzip=True)

    def register(self):
        """ Register the drainage divides"""
        slurplogger().info("Registering %s"%self.name)
        #possibly empty table
        self.truncateTable()
        fname=os.path.join(self.cacheDir(),self.fbase+".gz")
        #loop over  polygon entries
        for dicentry in IceSatPolygons(fname):
            self.addEntry(dicentry)


        self.updateInvent()




def DrainDivClassFactory(clsName,fbase):
    table=type(clsName +"Table", (draindivTBase,), {})
    return type(clsName, (IceSatDDivBase,), {"fbase": fbase, "table":table})


def getDrainDivDsets(conf):
    clslookup={"antarc_ddiv_icesat":"ant_full_drainagesystem_polygons.txt","green_ddiv_icesat":"grndrainagesystems_ekholm.txt","antarc_ddiv_icesat_grnd":"ant_grounded_drainagesystem_polygons.txt"}
    out=[]
    for clsName,fbase in clslookup.items():
        out.append(DrainDivClassFactory(clsName,fbase))
    return out

