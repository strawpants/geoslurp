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

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplog
from geoslurp.config.catalogue import geoslurpCatalogue

from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from sqlalchemy import Column,Integer,String,Float,Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB,ARRAY

from geoalchemy2.elements import WKBElement
from geoalchemy2.types import Geography,Geometry
from osgeo import ogr

from geoslurp.datapull import UriFile
from geoslurp.datapull.uri import findFiles
import os

from netCDF4 import Dataset as ncDset
from netCDF4 import num2date
import numpy as np
from datetime import datetime

geovertype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True, dimension=3)

@as_declarative(metadata=MetaData(schema='fesom'))
class FesomVertTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    onboundary=Column(Integer)
    topo=Column(Float)
    nodeid=Column(ARRAY(Integer))
    geom=Column(geovertype,nullable=False)

geotritype = Geography(geometry_type="POLYGONZ", srid='4326', spatial_index=True, dimension=3)

@as_declarative(metadata=MetaData(schema='fesom'))
class FesomTINTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    topo=Column(Float)
    cyclic=Column(Boolean)
    nodeid=Column(ARRAY(Integer))
    geom=Column(geotritype, nullable=False)

@as_declarative(metadata=MetaData(schema='fesom'))
class FESOMRunTBase(object):
    """Defines a FESOM table with output information"""
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id=Column(Integer,primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    interval=Column(String)
    uri=Column(String, index=True)
    data=Column(JSONB)


def FESOMMetaExtractor(uri):
    """Extract meta information from a FESOM output file"""
    slurplog.info("extracting data from %s"%(uri.url))

    try:
        ncFESOM=ncDset(uri.url)
    except OSError:
        slurplog.error("Cannot open netcdf file, skipping")
        return None
    tvar=ncFESOM["time"]

    if tvar.shape[0] == 0:
        #quick return 
        return None
    
    if tvar.calendar == "noleap":
        slurplog.warning("Note found 'noleap' calendar string but assuming 'standard'")
        cal='standard'
    else:
        cal=tvar.calendar

    #parse time
    time=num2date(tvar[:], tvar.units,cal,only_use_cftime_datetimes=False)
    # try to estimate the time step fromt he median
    deltamedian=np.median(np.diff(time))
    if deltamedian.days > 28 and deltamedian.days <= 32 :
        freq='monthly'
        #set tstart to the beginning of the month
        tstart=datetime(time[0].year,time[0].month,1)
    elif deltamedian.days >=1  and deltamedian.days <28:
        freq = "%ddaily" % (deltamedian.days)
        #remove the median time interval from the first time
        tstart=time[0]-deltamedian
    elif deltamedian.days < 1:
        freq = "%dhourly" % (deltamedian.seconds / 3600)
        #remove the median time interval from the first time
        tstart=time[0]-deltamedian



    data={"variables":{}}

    for ky,var in ncFESOM.variables.items():
        try:
            data["variables"][ky]=var.description
        except AttributeError:
            data["variables"][ky]=ky


    meta={"tstart":tstart,
          "tend":time[-1]+deltamedian,
          "lastupdate":uri.lastmod,
          "interval":freq,
          "uri":uri.url,
          "data":data
          }
    ncFESOM.close()
    return meta


class FESOMverticesBase(DataSet):
    """FESOM template table for vertices and triangle surface elements"""
    version=(0,0,0)
    scheme='fesom'
    def __init__(self,dbcon):
        super().__init__(dbcon)
        #setup table type
        self.table=type(self.name+"Table",(FesomVertTBase,),{})
        self.createTable()

    def pull(self):
        """No pulling functionality is incorporated"""
        pass

    def register(self,meshdir=None,abg=None):
        """register vertices in a FESOM mesh directory
        @param meshdir: directory where the mesh files reside
        @param abg non-standard Euler angles, defaults to [50, 15, -90]"""
        #only load when needed
        from pyfesom.load_mesh_data import fesom_mesh
        if not meshdir:
            raise RuntimeError("A meshdirectory needs to be supplied when registering this dataset")

        self.truncateTable()
        #load mesh from directory
        if abg:
            mesh=fesom_mesh(meshdir,abg=abg)
        else:
            mesh=fesom_mesh(meshdir)

        for id, (lon, lat, onbnd) in enumerate(zip(mesh.x2, mesh.y2, mesh.ind2d)):
            # create a point geometry
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(lon, lat)

            meta={"onboundary":int(onbnd),
                  "topo":mesh.topo[id],
                  "nodeid":[int(x - 1) for x in mesh.n32[id, :] if x >= 0],
                  "geom":WKBElement(point.ExportToIsoWkb(),srid=4326,extended=True)
                  }

            self.addEntry(meta)
            point=None

        self._dbinvent.data["Description"]="FESOM mesh vertices with 0-based indecises for the surface layers"
        self.setDataDir(os.path.abspath(meshdir))
        # self._dbinvent.data["meshdir"]=os.path.abspath(meshdir)
        self._dbinvent.data["zlevels"]=[zl for zl in mesh.zlevs]
        self.updateInvent()

class FESOMtinBase(DataSet):
    """FESOM template table for triangle surface elements"""
    version=(0,0,0)
    scheme='fesom'
    def __init__(self,dbcon):
        super().__init__(dbcon)
        #setup table type
        self.table=type(self.name+"Table",(FesomTINTBase,),{})
        self.createTable()

    def pull(self):
        """No pulling functionality is incorporated"""
        pass

    def register(self,meshdir=None,abg=None):
        """register triangular elements in a FESOM mesh directory
        @param meshdir: directory where the mesh files reside"""
        if not meshdir:
            raise RuntimeError("A meshdirectory needs to be supplied when registering this dataset")
        self.truncateTable()

        from pyfesom.load_mesh_data import fesom_mesh
        #load mesh from directory
        if abg:
            mesh=fesom_mesh(meshdir,abg=abg)
        else:
            mesh=fesom_mesh(meshdir)

        for i1, i2, i3 in mesh.elem:

            lons=mesh.x2[[i1,i2,i3]]
            # import ipdb;ipdb.set_trace()
            iscyclic=(lons.max() - lons.min() )> 100
            ring = ogr.Geometry(ogr.wkbLinearRing)
            tri = ogr.Geometry(ogr.wkbPolygon)

            ring.AddPoint(lons[0], mesh.y2[i1])
            ring.AddPoint(lons[1], mesh.y2[i2])
            ring.AddPoint(lons[2], mesh.y2[i3])
            #repeat the first point
            ring.AddPoint(lons[0], mesh.y2[i1])
            tri.AddGeometry(ring)
            meta={
                "nodeid":[int(i1), int(i2), int(i3)],
                "cyclic":iscyclic,
                "topo":(mesh.topo[i1]+mesh.topo[i2]+mesh.topo[i3])/3.0,
                "geom":WKBElement(tri.ExportToIsoWkb(),srid=4326,extended=True)
            }
            ring= None
            tri = None

            self.addEntry(meta)

        self._dbinvent.data["Description"]="FESOM mesh surface triangles with 0-based node indices"
        self.setDataDir(os.path.abspath(meshdir))
        # self._dbinvent.data["meshdir"]=os.path.abspath(meshdir)

        self.updateInvent()

class FESOMRunBase(DataSet):
    """FESOM template table for runs"""
    version=(0,0,0)
    scheme='fesom'
    grid=None
    def __init__(self,dbcon):
        super().__init__(dbcon)
        #setup table type
        self.table=type(self.name+"Table",(FESOMRunTBase,),{})
        self.createTable()
        #extract grid name from table name
        self.grid=self.name.split("_")[3]

    def pull(self):
        """No pulling functionality is incorporated"""
        pass

    def register(self,rundir=None,pattern='.*\.nc$'):
        """register FESOM netcdf output files
        @param rundir: directory where the netcdf files reside
        @param pattern: regular expression which the netcdfiles must obey defaults tkakes all files ending with nc"""
        if not rundir:
            raise RuntimeError("A directory/regex with output data needs to be supplied when registering this dataset")

        newfiles=self.retainnewUris([UriFile(file) for file in findFiles(rundir,pattern)])

        for uri in newfiles:
            meta=FESOMMetaExtractor(uri)
            if not meta:
                #don't register empty entries
                continue

            self.addEntry(meta)



        self._dbinvent.data["Description"]="FESOM output data table"
        self.setDataDir(os.path.abspath(rundir))
        self._dbinvent.data["grid"]=self.grid
        self.updateInvent()



def getFESOMDsets(conf):
    """Create dummy tables for displaying"""
    out=[]
    out.append(type("vertices_TEMPLATE", (FESOMverticesBase,), {}))
    out.append(type("triangles_TEMPLATE", (FESOMtinBase,), {}))
    out.append(type("run_TEMPLATE_g_TEMPLATE", (FESOMRunBase,), {}))
    return out

geoslurpCatalogue.addDatasetFactory(getFESOMDsets)
