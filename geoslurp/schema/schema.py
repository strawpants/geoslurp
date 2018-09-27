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
# from geoslurp.db import Inventory

class Schema(ABC):
    """Abstract base class for a schema.
    A schema is linked to a postgresql schema and manages the following:
    * Tables (datasets) in the schema
    * The schema's entry in the geoslurp inventory table
    * Postgresql functions

    To create a new schema inherit from this class and implement the abstractmethods
    """

    def __init__(self, geoslurpConn, conf):
        """Loads the inventory entry (if available and couples the schema to a database connection and configuration"""
        self._schema=self.__class__.__name__

        #store links to the configurator (allwows this
        self._conf=conf

        # We need to store the database links beacuse we need them in member functions
        self._db = geoslurpConn

        try:
            # retrieve the stored inventory entry
            self.dbinvent = self.db.getFromInventory(self.name)
            # convert version numer to tuple
            self.dbinvent.data["RGIversion"] = tuple(self.dbinvent.data["RGIversion"])
        except NoResultFound:
            # #set defaults for the  inventory
            self.dbinvent = Invent(datasource=self.name, pluginversion=self.pluginVersion,
                                   lastupdate=datetime.datetime.min, data={"RGIversion": (0, 0)})

            # create the schema
            self._db.CreateSchema(self._schema)
