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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022
import numpy as np
from sqlalchemy import Integer, String, Float, DateTime, LargeBinary,ARRAY,JSON,BIGINT
from geoslurp.types.numpy import datetime64Type

commonMap = {str: String, np.dtype(int): Integer, np.dtype(float):Float,dict:JSON, 
        np.int64:BIGINT, float:Float,np.float64:Float, "string": String, "integer": Integer, 
        np.datetime64:datetime64Type, np.dtype('<M8[ns]'):datetime64Type,np.str_:String}



