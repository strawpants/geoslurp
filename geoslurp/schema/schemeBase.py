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
# contains an abstract Base which all schemas need to follow


from abc import ABC, abstractmethod
from datetime import datetime
from sqlalchemy.orm.exc import NoResultFound
import shutil
import os

class Schema(ABC):
    """Abstract base class for a schema.
    A schema is linked to a postgresql schema and manages the following:
    * Tables (datasets) in the schema
    * The schema's entry in the geoslurp inventory table
    * Postgresql functions

    To create a new schema inherit from this class and implement the abstractmethods
    """

    __version__ = (0, 0, 0)
    __datasets__= {}
    def __init__(self, InventInstance, conf):
        """Loads the inventory entry (if available and couples the schema to a database connection and configuration"""
        self._schema=self.__class__.__name__

        #store links to the configurator (allows the class to get and set configuration changes)
        self._conf=conf
        self._db=InventInstance.db
        self.Dsets={}
        try:
            # retrieve the stored inventory entry
            self._dbinvent = InventInstance[self._schema]
        except NoResultFound:
            # #set defaults for the  inventory
            self._dbinvent = InventInstance.__table__(datasource=self._schema, pluginversion=self.__class__.__version__,
                                   lastupdate=datetime.min, data={})

            # create the schema
            InventInstance.db.CreateSchema(self._schema)


    def purge(self):
        """Delete the scheme, corresponding tables and data"""

        try:
            ddir = self._dbinvent.data["DataDir"]
            if os.path.isdir(ddir):
                shutil.rmtree(ddir)
        except KeyError:
            pass

        #now also remove the scheme and all tables/indexes
        try:
            self.db.dropSchema(self._schema,cascade=True)
        except:
            pass

    def __getitem__(self,dSetName):
        """Returns a Dataset instance by name"""
        return self.Dsets[dSetName]

    def __iter__(self):
        """loops over initiated Datasets in the scheme"""
        for val in self.Dsets.values():
            yield val

    def initDataSets(self,selectDsets=None):
        """Initializes selected datasets"""
        if not selectDsets:
            # default is to select all datasets
            selectDsets=list(self.__datasets__)

        for name in selectDsets:
            self.Dsets[name]=self.__datasets__[name](self)


def schemeFromName(name):
    return allSchemes()[name]

def allSchemes():
    """Returns a dictionary of available schemes"""
    return dict( [ (x.__name__,x) for x in Schema.__subclasses__()])

