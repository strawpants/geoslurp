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


from sqlalchemy.types import UserDefinedType, String

class OutDBZarrType(UserDefinedType):
    """Converts a column of xarray Dataarrays to an out-of-db data representation"""
    def __init__(self,zstore=None):
        self.zstore=zstore

    def get_col_spec(self, **kw):
        return "TEXT"

    def bind_processor(self, dialect):
        def process(value):
            """Stores an xarray DataArray to a zarr-archive and return a JSON with meta info"""

            return self.zstore
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return self.zstore
        return process


