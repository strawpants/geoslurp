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

from abc import ABC, abstractmethod

class DataSet(ABC):
    """Abstract Base class which hold a dataset (corresponding to a database table"""
    def __init__(self,scheme):
        self.name=self.__class__.__name__
        self._scheme=scheme
        #retrieve the Dataset entry (in the form of a dictionary) from the Inventory
        try:
            self._inventData=scheme._dbinvent.data[self.name]
        except KeyError:
            self._inventData={}

    def info(self):
        return self._inventData

    @abstractmethod
    def pull(self):
        """Pulls the necessary data from the online resource"""
        pass

    @abstractmethod
    def register(self):
        """Register the downloaded dataset in the database"""
        pass

    def purge(self):
        """Delete data and database entries"""
        pass

