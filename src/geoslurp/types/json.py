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

from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
import xarray as xr
from psycopg2.extras import Json
import os

class DataArrayJSONType(TypeDecorator):
    """Converts a column of xarray Dataarrays to an in db JSON representation"""
    impl = JSONB

    def process_bind_param(self, value, dialect):
        """Stores an xarray DataArray to a  JSON object"""
        if type(value) != xr.DataArray and type(value) != xr.Dataset:
            raise TypeError(f"Expected a xarray.DataArray or xarray.Dataset  got {type(value)}")
        
        return value.to_dict()

    def process_result_value(self, value, dialect):
            return xr.Dataset.from_dict(value)
