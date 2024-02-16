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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2021

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplog
import re
import pandas as pd
import geopandas as gpd
from sqlalchemy import Table,Column, Integer, String, Float, BigInteger,Date,DateTime, LargeBinary,ARRAY,JSON,BIGINT

from geoalchemy2.types import Geography,Raster
from geoalchemy2.elements import WKBElement,RasterElement
import shapely.wkb
from sqlalchemy import func
import numpy as np
from collections import namedtuple
from geoslurp.types.zarr import OutDBZarrType
from geoslurp.types.numpy import datetime64Type
import xarray as xr
import os
from datetime import datetime
#geoinfo=namedtuple("geoinfo",["srid","geoname","geomtype","dims","rastname"],defaults=(4326,"geom","GEOMETRY",2,"rast",))
#make compatible with 3.6
geoinfo=namedtuple("geoinfo",["srid","geoname","geomtype","dims","rastname"])
geoinfo.__new__.__defaults__=(4326,"geom","GEOMETRY",2,"rast",)

class PandasBase(DataSet):
    """Base class which reads in a pandas compatible table (CSV, excel, or in memory dataframe are currently supported) it in a db table"""
    pdfile=None
    skipfooter=0
    ftype="csv"
    encoding=None
    geoinfo=geoinfo()
    inbulk=False
    #how to treat series which have xarray dataarrays or datasets
    xrappend_dim=None
    def __init__(self,dbconn):
        super().__init__(dbconn)
    

    def setGeoInfo(self,df):
        """Try to extract srid, geometry type from a geopandas geodataframe"""

        if type(df) == gpd.GeoDataFrame:
            srid=df.crs.to_epsg()
            geoname=df.geometry.name
            
            geomtypes=df.geom_type.unique()
            if len(geomtypes) == 1:
                geomtype=geomtypes[0].upper()
            else:
                geomtype="GEOMETRY"

            if df.has_z.any():
                dims=3
            else:
                dims=2

            self.geoinfo=geoinfo(srid,geoname,geomtype,dims)

    

    def modify_df(self,df):
        """A derived type can overload this to make modifications to the dataframe before registering it in the database"""

        return df
    
    def columnsFromDataframe(self,df):
        """Returns a list of columns from a dataframe)"""
        Map = {str: String, np.dtype(int): Integer, 
                np.dtype(float):Float,dict:JSON,
                np.int64:BIGINT,
                float:Float,np.float64:Float,
                "string": String, "integer": Integer, 
                "floating":Float,
                xr.DataArray:OutDBZarrType(defaultZstore=self.outdbArchiveName(),modifyUri=self.conf.generalize_path,append_dim=self.xrappend_dim),
                xr.Dataset:OutDBZarrType(defaultZstore=self.outdbArchiveName(),modifyUri=self.conf.generalize_path,append_dim=self.xrappend_dim),
                "datetime64":DateTime,
                np.dtype('<M8[ns]'):datetime64Type}
        cols = [Column('id', Integer, primary_key=True)]
         
        for name,col in df.iteritems():
            if name == "id":
                #already added
                continue
            
            elif name == self.geoinfo.rastname:
                cType=Raster(spatial_index=False)
            elif name == self.geoinfo.geoname:
                if self.geoinfo.srid == 4326:
                    cType = Geography(geometry_type=self.geoinfo.geomtype, srid=self.geoinfo.srid, spatial_index=True,dimension=self.geoinfo.dims)
                else:
                    cType = Geometry(geometry_type=self.geoinfo.geomtype, srid=self.geoinfo.srid, spatial_index=True,dimension=self.geoinfo.dims)

            else:
                dtype=pd.api.types.infer_dtype(col,skipna=True)
                if dtype == "mixed":
                    val=col.loc[col.first_valid_index()]
                    if type(val) == np.ndarray:
                        #wrap the type in an array
                        cType=ARRAY(Map[val.dtype],dimensions=val.ndim)
                    else:
                        cType=Map[type(val)]
                else:
                    cType=Map[dtype]

            cols.append(Column(name,cType))
        return cols

    def registerInDatabase(self,df):
        self.setGeoInfo(df)        
        
        self.dropTable()
        if self.table == None:
            cols=self.columnsFromDataframe(df)
            self.createTable(cols)
        
        if self.inbulk:
            bulk=[]

        for k,row in df.iterrows():
            # possibly modify "geom" and "rast" entries so they can be consumed by the database
            if self.geoinfo.rastname in row:
                row[self.geoinfo.rastname]=func.ST_FromGDALRaster(bytes(row[self.geoinfo.rastname]),srid=self.geoinfo.srid)
            if self.geoinfo.geoname in row:
                row[self.geoinfo.geoname]=WKBElement(shapely.wkb.dumps(row[self.geoinfo.geoname]),srid=self.geoinfo.srid)
            
            # slurplog.info(f"Adding entry {k}")
            if self.inbulk:
                bulk.append(row) 
            else:
                self.addEntry(row)

        if self.inbulk:
            self.bulkInsert(bulk)

    def register(self,df=None):
        """Update/populate a database table from a pandas compatible file) 
    """
        if df is not None:
            #supplying an existing dataframe takes precedence
            indf=df.copy(deep=False)
        elif self.ftype == "csv":
            indf=pd.read_csv(self.pdfile,skipfooter=self.skipfooter,encoding=self.encoding)
        elif self.ftype == "excel":
            indf=pd.read_excel(self.pdfile,skipfooter=self.skipfooter,engine="openpyxl")

        else:
            raise RuntimeError("Don't know how to open %s, specify ftype"%(self.pdfile))
            #possibly modify dataframe in derived class 
        

        indf=self.modify_df(indf)
        
        slurplog.info("Filling pandas table %s.%s" % (self.scheme, self.name))
        
        self.registerInDatabase(indf)

        #also update entry in the inventory table
        self.updateInvent()

    def pull(self):
        """overload when needed"""
        pass


    def outdbArchiveName(self):
        # if self.stripuri:
            # arname=self.conf.generalize_path(os.path.join(self.dataDir(),self.name+"_data.zarr"))
        # else:
        arname=os.path.join(self.dataDir(),self.name+"_data.zarr")
        return arname
