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

class DataSetGeneric(DataSet):
    scheme="anyscheme"
    def __init__(self,dbcon):
        super().__init__(dbcon)

    def pull(self):
        raise NotImplementedError("No pull method defined for a generic dataset")
    def register(self):
        raise NotImplementedError("No register method defined for a generic dataset")

