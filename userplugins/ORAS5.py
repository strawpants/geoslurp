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

# Author Alisa Yakhontova (yakhontova@geod.uni-bonn.de), Roelof Rietbroek (r.rietbroek@utwente.nl), 2020

import numpy as np

from geoslurp.dataset import DataSet
import os
from datetime import datetime
from sqlalchemy import MetaData,Column,Float,Integer,String
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geography

from queue import Queue

from osgeo import ogr
from geoalchemy2.elements import WKBElement
from geoslurp.config.catalogue import geoslurpCatalogue
from netCDF4 import Dataset
from netCDF4 import num2date
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy.dialects.postgresql import  JSONB
from geoslurp.datapull.uri import findFiles
from geoslurp.datapull import UriFile
from geoslurp.config.slurplogger import slurplog
from netCDF4 import Dataset as ncDset



scheme='oras'

# =============================================================================
# 1. vertices
# =============================================================================
geopointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True, dimension=3)

orasVerticesTBase=declarative_base(metadata=MetaData(schema=scheme))

class orasVerticesTable(orasVerticesTBase):
    """Defines the PostgreSQL table"""
    __tablename__='oras5_meshgrid_025deg'
    id=Column(Integer,primary_key=True)
    depthlevel=Column(Float)
    # mask=Column(Integer)   
    geom=Column(geopointtype)


def orasVerticesMetaExtract(datadir, datafile,cachedir=False):
    """A little generator which extracts rows from """
    with Dataset(datadir+datafile, 'r') as nc_id:
        nav_lat=nc_id['nav_lat'][:]
        nav_lon=nc_id['nav_lon'][:]
        deptho_lev=nc_id['deptho_lev'][:]
        
        meta=[]
        for row in range(nc_id['nav_lat'][:].shape[0]):
            for column in range(nc_id['nav_lat'][:].shape[1]):
                # if mask[0,row,column]==1:
                point = ogr.Geometry(ogr.wkbPoint)
                point.AddPoint_2D(float(nav_lon[row,column]),float(nav_lat[row,column]))
        
                meta_entry=({"depthlevel":np.float(deptho_lev[row,column]),
                             "geom":WKBElement(point.ExportToIsoWkb(),srid=4326,extended=True)
                             })
                meta.append(meta_entry)
    return meta


class orasVerticesBase(DataSet):
    scheme=scheme
    version=(0,0,0)
    datafile='GLO-MFC_001_018_mask.nc'
    table=orasVerticesTable
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.updated=[]
        orasVerticesTBase.metadata.create_all(self.db.dbeng, checkfirst=True)
        self._uriqueue=Queue(maxsize=300)
        self._killUpdate=False
        self.thrd=None
        
        
    def pull(self):
            """No pulling functionality is incorporated"""
            pass

    def register(self, datadir=None):
        meta=orasVerticesMetaExtract(datadir,self.datafile)
        for i,meta_entry in enumerate(meta):
            self.addEntry(meta_entry)
        self._dbinvent.data["Description"]="ORAS5 mesh grid 0.25deg"
        self.setDataDir(os.path.abspath(datadir))
        self.updateInvent()



#%% =============================================================================
# 2. data files
# =============================================================================


@as_declarative(metadata=MetaData(schema=scheme))
class orasRunTBase(object):
    """Defines a table with output information"""
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id=Column(Integer,primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    uri=Column(String, index=True)
    data=Column(JSONB) # variables in the nc file

def orasMetaExtractor(uri):
    """Extract meta information from a output file"""
    slurplog.info("extracting data from %s"%(uri.url))

    try:
        nc_id=ncDset(uri.url)
    except OSError:
        slurplog.error("Cannot open netcdf file, skipping")
        return None
    tvar=nc_id["time_counter"]

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

    data={"variables":{}}

    for ky,var in nc_id.variables.items():
        try:
            data["variables"][ky]=var.description
        except AttributeError:
            data["variables"][ky]=ky


    meta={"tstart":datetime(time[0].year,time[0].month,1),
          "lastupdate":uri.lastmod,
          "uri":uri.url,
          "data":data
          }
    nc_id.close()
    return meta

class orasRunBase(DataSet):
    """template table for runs"""
    version=(0,0,0)
    scheme=scheme
    grid=None
    
    def __init__(self,dbcon):
        super().__init__(dbcon)
        self.table=type(self.name+"Table",(orasRunTBase,),{})
        self.createTable()
        
    def pull(self):
        """No pulling functionality is incorporated"""
        pass

    def register(self,rundir=None,pattern='.*\.nc$'):
        """register netcdf output files
        @param rundir: directory where the netcdf files reside
        @param pattern: regular expression which the netcdfiles must obey defaults tkakes all files ending with nc"""
        if not rundir:
            raise RuntimeError("A directory/regex with output data needs to be supplied when registering this dataset")

        newfiles=self.retainnewUris([UriFile(file) for file in findFiles(rundir,pattern)])

        for uri in newfiles:
            meta=orasMetaExtractor(uri)
            if not meta:
                #don't register empty entries
                continue

            self.addEntry(meta)



        self._dbinvent.data["Description"]="ORAS5 output data table"
        self.setDataDir(os.path.abspath(rundir))
        self._dbinvent.data["grid"]="025"
        self.updateInvent()
        
#%% =============================================================================
# dataset factory
# =============================================================================

def getOrasDsets(conf):
    """Create dummy tables for displaying"""
    out=[]
    out.append(type("vertices_TEMPLATE_TEMPLATE", (orasVerticesBase,), {})) # 'vertices_oras5_025'
    out.append(type("run_TEMPLATE_TEMPLATE_TEMPLATE_TEMPLATE", (orasRunBase,), {})) # 'run_oras5_temp_opa0_025'
    return out

geoslurpCatalogue.addDatasetFactory(getOrasDsets)

