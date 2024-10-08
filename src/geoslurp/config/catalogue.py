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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2019, ..,2024
import re
import sys
import os
import yaml
import inspect
from datetime import datetime
from geoslurp.config.slurplogger import slurplog
from geoslurp.db import Inventory
from importlib import import_module

from importlib_metadata import entry_points

from geoslurp.dataset.datasetgeneric import DataSetGeneric

class DatasetCatalogue:
    # holds factory methods for dyanamically building datasets
    __dscache__={}
    __dfcache__={}
    __dvcache__={}
    __dsplugsloaded__=False
    __dbfuncplugsloaded__=False
    __vwplugsloaded__=False
    def __init__(self):
        pass
    @classmethod
    def loadDatasetPlugins(cls,conf):
        """Adds news datasets, through the entry_points functionality"""
        if cls.__dsplugsloaded__:
            #no need to redo this
            return
            
        # load Dataset factories
        dsetgrpfac="geoslurp.dsetfactories"
        epsfactories=entry_points(group=dsetgrpfac)
        for entry in epsfactories:
            #expand the datasets in the factory and register them
            facfunc=entry.load()
            for dset in facfunc(conf):
                cls.__dscache__[dset.stname()]=dset
        
        #also directly load datasets (statically created)
        dsetgrp="geoslurp.dsets"
        epsdsets=entry_points(group=dsetgrp)
        for entry in epsdsets:
            #expand the dataset and register them
            dset=entry.load()
            cls.__dscache__[dset.stname()]=dset
        cls.__dsplugsloaded__=True
    
    def loadViewPlugins(self):
        """Add news views, through the entry_points functionality"""
        if self.__vwplugsloaded__:
            #no need to redo this
            return
            
        # load view factories
        vwgrpfac="geoslurp.viewfactories"
        epsfactories=entry_points(group=vwgrpfac)
        for entry in epsfactories:
            #expand the views in the factory and register them
            facfunc=entry.load()
            for vw in facfunc():
                self.__dvcache__[vw.svname()]=vw
        
        self.__vwplugsloaded__=True

    def loadDbfuncPlugins(self):
        """Adds news database functions, through the entry_points functionality"""
        if self.__dbfuncplugsloaded__:
            #no need to redo this
            return

        # load database functions
        dbfuncsgrp="geoslurp.dbfuncs"
        eps=entry_points(group=dbfuncsgrp)
        for dbfentry in eps:
            dbf=dbfentry.load()
            self.__dfcache__[dbf.sfname()]=dbf
        
        self.__dbfuncplugsloaded__=True
        
    @classmethod
    def listDataSets(cls,conf):
        cls.loadDatasetPlugins(conf)
        return cls.__dscache__.keys()

    def listFunctions(self,conf):
        self.loadDbfuncPlugins()
        return self.__dfcache__.keys()


    def listViews(self,conf):
        self.loadViewPlugins()
        return self.__dvcache__.keys()
            
    def getDsetClass(self,conf,name):
        """Loads a dataset as an class (but check cache first)"""
        self.loadDatasetPlugins(conf)

        if name in self.__dscache__:
            return self.__dscache__[name]
        else:
            schema=name.split(".")[0]
            raise KeyError(f"Dataset {name} not found (is the associated module '{schema}' installed?)")

        
    def getDFuncClass(self,conf,name):
        """Loads a database function as an class (but check cache first)"""
        self.loadDbfuncPlugins()
        if name in self.__dfcache__:
            return self.__dfcache__[name]
        else:
            raise KeyError(f"Custom Database function {name} not found")



    def getViewClass(self,conf,name):
        """Loads a database view as an class (but check cache first)"""
        self.loadViewPlugins()
        if name in self.__dvcache__:
            return self.__dvcache__[name]
        else:
           raise KeyError(f"Database view {name} not found")

