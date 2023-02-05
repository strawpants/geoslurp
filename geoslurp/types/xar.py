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
import os
import json
from datetime import datetime
import numpy as np
import pandas as pd

def custom_encoder(inval):
    if isinstance(inval,datetime):
        return inval.isoformat()
    if isinstance(inval,np.int64):
        return inval.item()
    else:
        type_name = inval.__class__.__name__
        raise TypeError(f"Object of type {type_name} is not serializable")

class XarDBType(UserDefinedType):
    """Converts a column of xarray group to a database column representation (JSON)"""
    def __init__(self,parentds,outofdb=None,groupby=None,writeoutofdb=True):
        self.outofdb=outofdb #filename of zarr store or None for in-database storage
        self.parentds=parentds
        self.groupby=groupby
        self.groupby_counter=0
        self.writeoutofdb=writeoutofdb
        indx=self.parentds.get_index(self.groupby)

        if type(indx) == pd.MultiIndex:
            self.slicenames=indx.names
        else:
            self.slicenames=None
            

    def get_col_spec(self, **kw):
        return "JSONB"

    def bind_processor(self, dialect):
        def process(value):
            """Stores an xarray DataArray or Dataset to a zarr-archive and return a JSON with meta info"""
            if type(value) != xr.DataArray and type(value) != xr.Dataset:
                raise TypeError(f"Expected a xarray DataArray/Dataset got {type(value)}")
    
            if self.outofdb is not None:
                
                if self.groupby_counter == 0 and self.writeoutofdb:
                    #save parentds to a zarr store
                    if self.groupby in self.parentds.xindexes:
                        self.parentds.reset_index(self.groupby).to_zarr(self.outofdb,mode='a')
                    else:
                        self.parentds.to_zarr(self.outofdb,mode='a')
                self.groupby_counter+=1
                if self.slicenames is None:
                    metadict=json.dumps({"uri":self.outofdb,"slice":{self.groupby:value[self.groupby].item()}},default=custom_encoder)
                else:
                    metadict=json.dumps({"uri":self.outofdb,"slice":{self.groupby:{ky:vl for ky,vl in zip(self.slicenames, value[self.groupby].item())}}},default=custom_encoder)

            else:
                metadict=json.dumps(value.to_dict(),default=custom_encoder)
            return metadict
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value
        return process


