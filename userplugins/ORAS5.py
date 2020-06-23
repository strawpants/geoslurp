#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 09:25:09 2020

@author: alisa
"""


from netCDF4 import Dataset
from netCDF4.utils import ncinfo
import numpy as np
from sys import getsizeof
from netCDF4 import num2date
import matplotlib.pyplot as plt



# =============================================================================
# download and register oras5 in geoslurp
# =============================================================================


# #%% =============================================================================
# # some functions
# # =============================================================================
# def sec2month(nc_id, varname):
#     times=nc_id[varname]
#     units=times.units
#     #calendar=times.calendar
#     tt=[entry for entry in num2date(times[:],units)]
     
#     return tt

# #%% =============================================================================
# # get to know the data
# # /shares/nis/data1/bernd/oras5/
# # =============================================================================

# tempfile='/shares/nis/data1/bernd/oras5/temp/opa0/votemper_ORAS5_1m_201101_grid_T_02.nc'

# temp_var=[]
# with Dataset(tempfile, 'r') as nc_id:  # open file and obtain its id
#     deptht=nc_id['deptht'][:]
    
#     temp_var = nc_id['votemper']
#     fillvalue= temp_var._FillValue
#     temp_data=temp_var[:]
#     temp_data [temp_data == fillvalue] = np.nan # change the fill value
#     temp_data =np.squeeze(temp_data)
#     data_small = temp_data[0,100:200,0:100]

    
#     time = sec2month(nc_id,'time_counter')
    
#     # for i in nc_id.variables: #file info
#     #     temp_var.append(nc_id.variables[i].name)
        
#     #     print(i, nc_id.variables[i])
#     #     print(' ')     

# # print(getsizeof(data))

# print('------') 

# #%%
# coordfile='/shares/nis/data1/bernd/oras5/grid/GLO-MFC_001_018_coordinates.nc'
# with Dataset(coordfile, 'r') as nc_id:  # open file and obtain its id
#     e1t=nc_id['e1t'][:]
#     e2t=nc_id['e2t'][:]
#     e3t_0=nc_id['e3t_0'][:]
#     e3t_ps=nc_id['e3t_ps'][:]
    
#     # for i in nc_id.variables: #file info
#     #     print(i, nc_id.variables[i])
#     #     print(' ') 

# print('------') 

# #%%    
# maskfile='/shares/nis/data1/bernd/oras5/grid/GLO-MFC_001_018_mask.nc'
# with Dataset(maskfile, 'r') as nc_id:  # open file and obtain its id
#     nav_lat=nc_id['nav_lat'][:]
#     deptht=nc_id['deptht'][:]
#     nav_lon=nc_id['nav_lon'][:]
#     mask=nc_id['mask'][:]
#     deptho_lev=nc_id['deptho_lev'][:]
    
#     # for i in nc_id.variables: #file info
#     #     print(i, nc_id.variables[i])
#     #     print(' ') 
    
# #%%
# plt.pcolor(deptho_lev, cmap= 'magma')
# plt.colorbar()
# plt.show()


#%% =============================================================================
# define geoslurp table structure
# 0. add to inventory
#     scheme='oras'
#     dataset='oras5_temp_opa0',....
#     lastupdate
#     version
#     cache
#     datadir='/shares/nis/data1/bernd/oras5/temp/'....
#     data={'grid':'0.25', 'Description':'0 ensemble....'}
#    
# 1. vertices
#     for every grid point with mask=1
#         geom point
#         depth level
#        
# 2. temp. files / sal. files (separately)
#     for every file:
#         uri
#         tstart
#         last update
#         data (variable and description)
#       
# querying:
#     from the vertices table
#         indices of locations 
#         number of depth levels for these locations
#     from the files table based on timespan
#         apply indices to the variables
#
# =============================================================================

from geoslurp.dataset import DataSet
from geoslurp.datapull.http import Uri as http
import os
from datetime import datetime
from sqlalchemy import MetaData,Column,Float,Integer,String,BigInteger
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geography

from geoslurp.config import setInfoLevel
from geoslurp.db import geoslurpConnect
from shapely.geometry import Point
from shapely.wkb import dumps, loads
from queue import Queue

from osgeo import ogr
from geoalchemy2.elements import WKBElement
from geoslurp.config.catalogue import geoslurpCatalogue

# =============================================================================
# 1. vertices
# =============================================================================
# dont use pointz: https://github.com/geoalchemy/geoalchemy2/issues/233
# geopointtype = Geography(geometry_type="POINT", srid='4326', spatial_index=True,dimension=3)
# datadir='/shares/nis/data1/bernd/oras5/grid/'
# datafile='GLO-MFC_001_018_mask.nc'

# scheme='oras'

# # Setup the postgres table using methods as specified with sqlalchemy
# orasVerticesTBase=declarative_base(metadata=MetaData(schema=scheme))

# class orasVerticesTable(orasVerticesTBase):
#     """Defines the PostgreSQL table"""
#     __tablename__='oras5_meshgrid_025deg'
#     id=Column(Integer,primary_key=True)
#     depthlevel=Column(Float)
#     geom=Column(geopointtype)


# def orasVerticesMetaExtract(datadir, datafile,cachedir=False):
#     """A little generator which extracts rows from """
#     with Dataset(datadir+datafile, 'r') as nc_id:
#         nav_lat=nc_id['nav_lat'][:]
#         # deptht=nc_id['deptht'][:]
#         nav_lon=nc_id['nav_lon'][:]
#         mask=nc_id['mask'][:]
#         deptho_lev=nc_id['deptho_lev'][:]
        
#         meta=[]
#         for row in range(nc_id['nav_lat'][:].shape[0]):
#             for column in range(nc_id['nav_lat'][:].shape[1]):
#                 if mask[0,row,column]==1:
#                     # point=Point(nav_lon[row,column], nav_lat[row,column])
                    
#                     point = ogr.Geometry(ogr.wkbPoint)
#                     point.AddPoint(float(nav_lon[row,column]),float(nav_lat[row,column]),0)
            
#                     meta_entry=({"depthlevel":np.float(deptho_lev[row,column]),
#                                   # "geom":point.wkb_hex})
#                                    "geom":WKBElement(point.wkb,srid=4326,extended=True)})
#                     meta.append(meta_entry)
#     return meta


# # =============================================================================
# # # INFO: transform point formats
# # # >> from shapely.wkb import dumps, loads
# # # >>> wkb = dumps(Point(0, 0))
# # # >>> wkb.encode('hex')
# # # '010100000000000000000000000000000000000000'
# # # >>> loads(wkb).wkt
# # # 'POINT (0.0000000000000000 0.0000000000000000)'
# # =============================================================================



# class oras5_meshgrid_025deg(DataSet):
#     scheme=scheme
#     version=(0,0,0)
#     datadir='/shares/nis/data1/bernd/oras5/grid/'
#     datafile='GLO-MFC_001_018_mask.nc'
#     table=orasVerticesTable
#     def __init__(self,dbcon):
#         super().__init__(dbcon)
#         self.updated=[]
#         orasVerticesTBase.metadata.create_all(self.db.dbeng, checkfirst=True)
#         self._uriqueue=Queue(maxsize=300)
#         self._killUpdate=False
#         self.thrd=None
        
        
#     def pull(self):
#             """No pulling functionality is incorporated"""
#             pass

#     def register(self):
#         self.truncateTable()

#         #insert by entry
#         # print(self.datadir)
#         # print(self.datafile)
#         # print(orasVerticesMetaExtract(self.datadir,self.datafile))
#         meta=orasVerticesMetaExtract(self.datadir,self.datafile)
#         for i,meta_entry in enumerate(meta):
#             self.addEntry(meta_entry)
#             # print(i)
        # self._dbinvent.data["Description"]="ORAS5 mesh grid 0.25deg"
        # self._dbinvent.data["depthlevels"]=
  
#         self.updateInvent()



# geoslurpCatalogue.addDataset(oras5_meshgrid_025deg)

# setInfoLevel()
# gpcon=geoslurpConnect(readonlyuser=False)
# orasVertices=oras5_meshgrid_025deg(gpcon)
# orasVertices.register()

#%% =============================================================================
# 2. temp files
# =============================================================================
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy.dialects.postgresql import  JSONB,ARRAY
from geoslurp.datapull.uri import findFiles
from geoslurp.datapull import UriFile
from geoslurp.config.slurplogger import slurplog
from netCDF4 import Dataset as ncDset

scheme='oras'

@as_declarative(metadata=MetaData(schema=scheme))
class orasRunTBase(object):
    """Defines a table with output information"""
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        # return cls.__name__[:-5].lower()
        return cls.__name__.lower()
    id=Column(Integer,primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    # tend=Column(TIMESTAMP)
    # interval=Column(String)
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
    # rundir='/shares/nis/data1/bernd/oras5/temp/opa0/'
    rundir='/home/alisa/Div/test/test/oras5/temp/opa0/'
    rundir_split = rundir.split("/")
    tablename = rundir_split[5]+"_"+rundir_split[6]+"_"+rundir_split[7]
    # name=name 
    # print(name)
    
    def __init__(self,dbcon):
        self.name = self.tablename
        super().__init__(dbcon)
        #setup table type
        # self.name = tablename
        print(self.name)
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
        self._dbinvent.data["grid"]=0.25
        self.updateInvent()
        

setInfoLevel()
gpcon=geoslurpConnect(readonlyuser=False)
orasRuns=orasRunBase(gpcon)
orasRuns.register(rundir='/home/alisa/Div/test/test/oras5/temp/opa0/')


#%% catalogue

def getOrasDsets(conf):
    """Create dummy tables for displaying"""
    out=[]
    # out.append(type("run_TEMPLATE_g_", (orasRunBase,), {}))
    out.append(type("oras5run_TEMPLATE_TEMPLATE", (orasRunBase,), {}))
    return out

geoslurpCatalogue.addDatasetFactory(getOrasDsets)

# # from geoslurp.config.catalogue import geoslurpCatalogue
# from geoslurp.db import Settings
# # from geoslurp.config import setInfoLevel

# # from geoslurp.db import geoslurpConnect

# setInfoLevel()

# gpcon=geoslurpConnect(readonlyuser=False) # this will be a connection based on the readonly userfrom geoslurp.db.geo

# #Some datasets need info from the server side settings so we need to load these
# conf=Settings(gpcon)

# for dsclass in geoslurpCatalogue.getDatasets(conf,""):
#     # create an instance of the class
#     dsobject=orasRunBase(gpcon)
#     # dsobject.pull()
#     dsobject.register(rundir='/home/alisa/Div/test/test/oras5/temp/opa0/')

