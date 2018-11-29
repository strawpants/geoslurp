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

    __datasets__= {}
    def __init__(self, InventInstance, conf):
        """Loads the inventory entry (if available and couples the schema to a database connection and configuration"""
        self._schema=self.__class__.__name__.lower()

        #store links to the configurator (allows the class to get and set configuration changes)
        self.conf=conf
        self.db=InventInstance.db
        self.Inventory=InventInstance
        self.Dsets={}
        self._ses=self.db.Session()
        try:
            # retrieve the stored inventory entry
            # self._dbinvent = InventInstance[self._schema]
            self._dbinvent=self._ses.query(InventInstance.__table__)\
                .filter(InventInstance.__table__.scheme == self._schema).one()
            self.hasInventEntry=True
        except NoResultFound:
            # #set defaults for the  inventory
            self._dbinvent = InventInstance.__table__(scheme=self._schema, datasets={}, pgfuncs={})

            # create the schema
            self.db.CreateSchema(self._schema)
            self.hasInventEntry=False

    def updateInvent(self,name, datdict=None, funcdict=None):
        """Update the entries in the inventory table by means of """
        if datdict:
            self._dbinvent.datasets[name]=datdict

        if funcdict:
            self._dbinvent.pgfuncs=funcdict

        if not self.hasInventEntry:
            self._ses.add(self._dbinvent)
            self.hasInventEntry=True

        self._ses.commit()


    def purge(self):
        """Delete the scheme, corresponding tables and data"""

        try:
            ddir = self.conf.getDir(self._schema,dirEntry='DataDir')
            if os.path.isdir(ddir):
                shutil.rmtree(ddir)
        except KeyError:
            pass

        #now also remove the scheme and all tables/indexes
        try:
            self.db.dropSchema(self._schema,cascade=True)
        except:
            pass

        #finally remove the entry from the inventory
        if self.hasInventEntry:
            self._ses.delete(self._dbinvent)
            self._ses.commit()

    def __getitem__(self,dSetName):
        """Returns a Dataset instance by name"""
        return self.Dsets[dSetName]

    def __iter__(self):
        """loops over initiated Datasets in the scheme"""
        for val in self.Dsets.values():
            yield val

    def initDataSets(self,selectDsets=None):
        """Initializes selected datasets"""
        if not self.__datasets__:
            self.initDsetClasses(self.conf)

        if not selectDsets:
            # default is to select all datasets
            selectDsets=list(self.__datasets__)

        for name in selectDsets:
            self.Dsets[name]=self.__datasets__[name](self)

    def dropTable(self, tableName):
        """Convenience function to drop a table contained within this schema
         :param tableName (str) : name of the table to be dropped"""
        self.db.dropTable(tableName,self._schema)

    @classmethod
    def listDsets(cls,conf):
        """List the names of the datasets (possibly dynamically retrieved)"""
        cls.initDsetClasses(conf)
        return cls.__datasets__.keys()

    @classmethod
    def initDsetClasses(cls,conf):
        """This can be overriden by derived classes to allow for the dynamic retrieval of datasets"""
        pass

def schemeFromName(name):
    return allSchemes()[name]

def allSchemes():
    """Returns a dictionary of available schemes"""
    return dict( [ (x.__name__,x) for x in Schema.__subclasses__()])


def mergeDicts(dict1,*vardict):
    """Convenience function which allows inline merging of dicts for python versions < 3.5 (3.5 supports {**dict1,**dict2}"""
    x=dict1.copy()
    for dictadd in vardict:
        x.update(dictadd)
    return x
