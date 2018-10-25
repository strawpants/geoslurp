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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018
# Scheme or the Global self consistent Hierachical High resolution geography database

import os
from geoslurp.schema import Schema
from geoslurp.config import getCreateDir
from geoslurp.dataset import getGSHHGdict

class GSHHG(Schema):
    """A scheme which contains the datasets from the Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    __datasets__=getGSHHGdict()
    __version__=(0, 0, 0)
    def __init__(self,InventInstance, conf):
        super().__init__(InventInstance, conf)
        self.cache=getCreateDir(os.path.join(conf["CacheDir"],"GSHHG"))


