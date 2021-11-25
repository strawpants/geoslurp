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
from sqlalchemy.types import DateTime
import numpy as np
from datetime import datetime

def np_to_datetime(value):
        if not hasattr(value,'dtype'):
            raise TypeError(f"Expected a np.datetime64  got {type(value)}")
        if value.dtype == np.dtype('datetime64[ns]'):
            res=1e-9
        elif value.dtype == np.dtype('datetime64[us]'):
            res=1e-6
        elif value.dtype == np.dtype('datetime64[ms]'):
            res=1e-3
        elif value.dtype == np.dtype('datetime64[s]'):
            res=1
        else:
            raise TypeError(f"Expected a np.datetime64[..]  got {value.dtype})")
        return datetime.utcfromtimestamp(value.astype(int)*res)

class datetime64Type(TypeDecorator):
    """Converts a column of numpy datetime64[ns] to a DateTime representation"""
    impl = DateTime

    def process_bind_param(self, value, dialect):
        """Converts a numpy datetime64 object to a python datetime"""
        return np_to_datetime(value)

    def process_result_value(self, value, dialect):
            return np.datetime64(value)
