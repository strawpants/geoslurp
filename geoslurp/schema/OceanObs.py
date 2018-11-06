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
# Schema which stores Argo data (links)
from geoslurp.schema import Schema
from geoslurp.dataset import Argo

class OceanObs(Schema):
    """A scheme to download and manage oceanographic insitu data.
    Currently the following datasets can be registered:
    Argo data from the CORLIOLIS Opendap server
    """
    __datasets__={"Argo":Argo}
    def __init__(self,InventInstance, conf):
        super().__init__(InventInstance, conf)




