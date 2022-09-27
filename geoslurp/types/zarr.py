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


from sqlalchemy.types import UserDefinedType
import xarray as xr
from psycopg2.extras import Json
import os

def getAttr(ds,key):
    if key in ds.attrs:
        return ds.attrs[key]
    else:
        return None

def get_storage(ds):
    return getAttr(ds,"gslrp_storage")

def get_append_dim(ds):
    return getAttr(ds,"gslrp_append_dim")

class OutDBZarrType(UserDefinedType):
    """Converts a column of xarray Dataarrays to an out-of-db data representation"""
    def __init__(self,defaultZstore=None,modifyUri=None,append_dim=None):
        self.defaultZstore=defaultZstore
        self.modifyUri=modifyUri
        self.append_dim=append_dim

    def get_col_spec(self, **kw):
        return "JSONB"

    def bind_processor(self, dialect):
        def process(value):
            """Stores an xarray DataArray to a zarr-archive and return a JSON with meta info"""
            if type(value) != xr.DataArray and type(value) != xr.Dataset:
                raise TypeError(f"Expected a xarrayDataArray/Dataset got {type(value)}")
           
            storage=self.defaultZstore
            
            #find out whether this dataset needs to be appended to an existing
            append_dim=get_append_dim(value)
            if not self.append_dim:
                #Expand a dimension and add a coordinate for lookup
                append_dim="gslrp"
                value=value.expand_dims({append_dim:1})
            else:
                append_dim=self.append_dim
            vname=value.name

            if type(value) == xr.DataArray:
                value=value.to_dataset()

            if os.path.exists(storage):
                #append
                zstore=xr.open_zarr(storage)
                iappend=zstore.sizes[append_dim]+1
                value.to_zarr(storage,mode='a',append_dim=append_dim)
            else:
                #start with a new file
                iappend=1
                value.to_zarr(storage,mode='w')
            if self.modifyUri:
                storage=self.modifyUri(storage)
                
            metadict=Json({"uri":storage,"varnames":[vname],"slice":{append_dim:iappend}})
            return metadict
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value
        return process


