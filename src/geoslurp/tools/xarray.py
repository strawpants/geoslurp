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



import xarray as xr
from geoslurp.dataset.xarraybase import XarrayBase
from geoslurp.db import Settings
import os
import shutil
import numpy as np
import re

@xr.register_dataarray_accessor("gslrp")
class XarGeoslurp:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    @property
    def storage(self):
        if "gslrp_storage" in self._obj.attrs:
            return self._obj.attrs["gslrp_storage"]
        else:
            return None

    @storage.setter
    def storage(self,uri):
        self._obj.attrs["gslrp_storage"]=uri
    
    @staticmethod
    def expand_dim_impl(ds,name,value):
        """Expands the data array with a 1-sized dimension and add a coordinate value"""
        ds2=ds.expand_dims({name:1})
        ds2=ds2.assign_coords({name:[value]})
        ds2.gslrp.append_dim=name

        return ds2
    
    def expand_dim(self,name,value):
        return XarGeoslurp.expand_dim_impl(self._obj,name,value)

    @property
    def append_dim(self):
        if "gslrp_append_dim" in self._obj.attrs:
            return self._obj.attrs["gslrp_append_dim"]
        else:
            return None

    @append_dim.setter
    def append_dim(self,append_dim):
        self._obj.attrs["gslrp_append_dim"]=append_dim

    def join_at(self,**kwargs):
       return XarGeoslurp.join_at_impl(self._obj,kwargs)

    @staticmethod
    def join_at_impl(ds,**kwargs):
        """When reading/writing to geoslurp, join this datarray with others at this dimension/coordinate"""
        if len(kwargs) != 1:
            raise TypeError( "connectAt only accepts one argument)")

        for ky,val in kwargs.items():
            if ky in ds.coords:
                pass
            else:
                #add a new dimensions and coordinate (dim and coord have the same name)
                ds.gslrp.append_dim=ky
                ds2=ds.expand_dims({ky:1})
                ds2=ds.assign_coords({ky:[val]})
        return ds2

    def save(self,gsconn,tablename,groupby,schema="public",outofdb=False,overwrite=False):
        if not self._obj.name:
            self._obj.name=tablename
        self._obj.to_dataset().gslrp.save(gsconn,tablename,groupby,schema,outofdb,overwrite)

@xr.register_dataset_accessor("gslrp")
class XarDsAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
    
    @property
    def storage(self):
        if "gslrp_storage" in self._obj.attrs:
            return self._obj.attrs["gslrp_storage"]
        else:
            return None

    @storage.setter
    def storage(self,uri):
        self._obj.attrs["gslrp_storage"]=uri
    
    def expand_dim(self,name,value):
        return XarGeoslurp.expand_dim_impl(self._obj,name,value)
    
    @property
    def append_dim(self):
        if "gslrp_append_dim" in self._obj.attrs:
            return self._obj.attrs["gslrp_append_dim"]
        else:
            return None

    @append_dim.setter
    def append_dim(self,append_dim):
        self._obj.attrs["gslrp_append_dim"]=append_dim
    
    def join_at(self,**kwargs):
       return XarGeoslurp.join_at_impl(self._obj,kwargs)

    def save(self,gsconn,tablename,groupby,schema="public",outofdb=False,overwrite=False):
        """Saves an xarray object to a geoslurp database"""
        TableClass=type(tablename,(XarrayBase,),{"scheme":schema,"groupby":groupby,"outofdb":outofdb})
        xrTable=TableClass(gsconn)
        if overwrite:
            xrTable.dropTable()
            if outofdb:
                zarrar=xrTable.outdbArchiveName()
                if os.path.exists(zarrar):
                    shutil.rmtree(zarrar)

        xrTable.register(ds=self._obj)
    
    @staticmethod
    def load(gsconn,qry,xrcol="data"):
        """Load xarray dataset and auxiliary columns from a table into a new xarray dataset"""
        # select all if the table looks like a table name [scheme.]tablename
        if re.search(r'^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$',qry):
            #take the whole table
            qryres=gsconn.dbeng.execute(f"SELECT * FROM {qry}")
        else:
            qryres=gsconn.dbeng.execute(qry)
        dsout=None
        outofdb=False
        cols=[]
        for row in qryres:
            if dsout is None:
                #first row
                
                indexnames=[ky for ky in row.iterkeys() if ky != xrcol]
                if "uri" in row[xrcol]:
                    outofdb=True

                if outofdb:
                    zstore=row[xrcol]["uri"]
                    if 'LOCALDATAROOT' in zstore:
                        conf=Settings(gsconn)
                        zstore=conf.get_local_path(zstore)
                    if zstore.endswith('.zarr'):
                        dsout=xr.open_zarr(zstore,consolidated=False)
                    else:
                        dsout=xr.open_dataset(zstore)
                    #currently assumes all data is in the provided uri
                else:
                    dsout=xr.Dataset.from_dict(row[xrcol])
            else:
                if not outofdb:
                    dsout=xr.concat([dsout,xr.Dataset.from_dict(row[xrcol])],indexnames[0])
            #also assemble list of auxialiary columns as a dictionary
            cols.append({ky:row[ky] for ky in indexnames})
        if "time" in dsout and not outofdb:
            #convert time data to np datetime64
            dsout["time"]=[np.datetime64(ts) for ts in dsout.time.data]
        
        return cols,dsout

