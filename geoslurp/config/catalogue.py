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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019
import re
import sys
import os
import yaml
import inspect
from datetime import datetime
from geoslurp.config.slurplogger import slurplog
from geoslurp.db import Inventory
from importlib import import_module

from geoslurp.dataset.datasetgeneric import DataSetGeneric

class DatasetCatalogue:
    #holds dataset classes (not initiated!)
    __dsets__=[]
    __dbfuncs__=[]
    __dviews__=[]
    # holds factory methods for dyanamically building datasets
    __dsetfac__=[]
    __catalogue__=None
    __dscache__={}
    __dfcache__={}
    __dvcache__={}
    __pluginpaths__=None
    def __init__(self):
        #Set default plugin path (may have entries appended)
        self.__pluginpaths__=[os.path.abspath(os.path.dirname(__file__)+"../../../geoslurp_userplugins")] 
    
    @staticmethod 
    def getCacheFile(conf):
        return os.path.join(conf.getCacheDir("Dset"),"Dset_Catalogue.yaml")
    
    def setUserPlugPaths(self,plugindirs,append=True):
        if append:
            self.__pluginpaths__.append(plugindirs)
        else:
            self.__pluginpaths__=plugindirs

    def addDataset(self, datasetcls):
        self.__dsets__.append(datasetcls)
    
    def addDatasetFactory(self, datasetclsfac):
        self.__dsetfac__.append(datasetclsfac)
    
    def addDbFunc(self,dfclass):
        self.__dbfuncs__.append(dfclass)

    def addView(self,dvclass):
        self.__dviews__.append(dvclass)

    def refresh(self,conf):
        """Refresh the dataset catalogue"""

        self.registerAllDataSets(conf)
        
        #load inventory of existing datasets (for the templated)
        Inv=Inventory(conf.db)

        self.__catalogue__={"datasets":{},"factories":{},"functions":{},"views":{}}
        #loop over dataset in the factories

        for ds in self.__dsets__:
            name=".".join([ds.scheme,ds.__name__])
            if re.search("TEMPLATE",name):
                srch=re.sub("TEMPLATE","([^\\\s]+)",name.replace(".","\."))
                #possibly also add existing datasets so they can be found by regular expressions
                for entry in Inv:
                    if not entry.dataset:
                        continue
                    nameexisting=".".join([entry.scheme,entry.dataset])
                    if re.search(srch,nameexisting):
                        #add 
                        self.__catalogue__["datasets"][nameexisting]={"template":name}
            self.__catalogue__["datasets"][name]={"module":ds.__module__}
            self.__dscache__[name]=ds

        for dsfac in self.__dsetfac__:
            self.__catalogue__["factories"][dsfac.__name__]={"module":dsfac.__module__}
            for ds in dsfac(conf):
                name=".".join([ds.scheme,ds.__name__])
                if re.search("TEMPLATE",name):
                    srch=re.sub("TEMPLATE","([^\\\s]+)",name.replace(".","\."))
                    #possibly also add existing datasets so they can be found by regular expressions
                    
                    for entry in Inv:
                        if not entry.dataset:
                            continue
                        nameexisting=".".join([entry.scheme,entry.dataset])
                        if re.search(srch,nameexisting):
                            #add 
                            self.__catalogue__["datasets"][nameexisting]={"template":name}
                
                self.__catalogue__["datasets"][name]={"factory":dsfac.__name__}
                self.__dscache__[name]=ds
        
        for df in self.__dbfuncs__:
            name=".".join([df.scheme,df.__name__])
            self.__catalogue__["functions"][name]={"module":df.__module__}
            self.__dfcache__[name]=df


        for dv in self.__dviews__:
            name=".".join([dv.scheme,dv.__name__])
            self.__catalogue__["views"][name]={"module":dv.__module__}
            self.__dvcache__[name]=dv

        # add plugin paths before saving
        #save to yaml
        
        self.__catalogue__["lastupdate"]=datetime.now()
        self.__catalogue__["pluginpaths"]=self.__pluginpaths__
         

        cachefile=self.getCacheFile(conf)
        slurplog.info("saving available Datasets, functions and views to catalogue %s"%cachefile)
        with open(cachefile,'wt') as fid:
            yaml.dump(self.__catalogue__, fid, default_flow_style=False)
    
    def loadCatalogue(self,conf):
        if self.__catalogue__:
            return
        
        cachefile=self.getCacheFile(conf)
        if not os.path.exists(cachefile):
            self.refresh(conf)
        with open(cachefile,'rt') as fid:
            self.__catalogue__=yaml.safe_load(fid)
            #extract user plugin paths
            self.__pluginpaths__=self.__catalogue__["pluginpaths"]

    def registerAllDataSets(self,conf):
        """load all dataset classes (but don't construct them)"""
        if self.__dsets__:
            #already loaded (quick return)
            return
        
        #dynamically import all relevant datasets and class factories (including userplugin datasets)
       
        #dynamically load functions
        modgeof=__import__("geoslurp.dbfunc")

        #dynamically load views
        modgeov=__import__("geoslurp.view")

        #also load datasets from userplugin directories
        for upath in self.__pluginpaths__:
            if sys.path.count(upath) == 0:
                sys.path.append(upath)
                mod=__import__(os.path.basename(upath))


    def listDataSets(self,conf):
        self.loadCatalogue(conf)
        return self.__catalogue__["datasets"]

    def listFunctions(self,conf):
        self.loadCatalogue(conf)
        return self.__catalogue__["functions"]

    def listFactories(self,conf):
        self.loadCatalogue(conf)
        return self.__catalogue__["factories"]

    def listViews(self,conf):
        self.loadCatalogue(conf)
        return self.__catalogue__["views"]
            
    def getDsetClass(self,conf,name):
        """Loads a dataset as an class (but check cache first)"""
        if name in self.__dscache__:
            return self.__dscache__[name]
        else:
           self.loadCatalogue(conf) 
           dsentry=self.__catalogue__["datasets"][name]
           #load isolated class
           if "factory" in dsentry:
               #produce all classes in the factory
               facentry=self.__catalogue__["factories"][dsentry["factory"]]
               facmod=import_module(facentry["module"])
               fac=getattr(facmod,dsentry["factory"])
               for ds in fac(conf):
                   self.__dscache__[".".join([ds.scheme,ds.__name__])]=ds
               return self.__dscache__[name]
           elif "template" in dsentry:
                #the requested class must be derived from a templated base class
                dsbase=self.getDsetClass(conf,dsentry["template"])
                scheme,tbl=name.split(".")
                ds=type(tbl,(dsbase,),{"scheme":scheme})
                self.__dscache__[name]=ds
                return ds
           else:
               mod=import_module(dsentry["module"])
               # import ipdb;ipdb.set_trace()
               ds=getattr(mod,name.split(".")[1])
               self.__dscache__[name]=ds
               return ds
        
    def getDatasets(self,conf,regex):
        """retrieves a list of dataset classes possibly obeying a certain regex"""
        self.loadCatalogue(conf)        
        #get the valid names  
        outdsets=[] 
        regexcomp=re.compile(regex)
        #we expect to have only one matching entry when the regex is a fully-qualified scheme.table name
        singleEntry=re.fullmatch("^[\w]+(?:\.[\w]+)$",regex)

        for name in self.__catalogue__["datasets"].keys():
            ds=None
            if regexcomp.fullmatch(name):
                ds=self.getDsetClass(conf,name)
                if singleEntry:
                    if not outdsets:
                        outdsets=[None]
                    #this possibly overwrites a fitting template class
                    outdsets[0]=ds
                    #no need to look any further we found our fit
                    break
                else:
                    outdsets.append(ds)
                    #note: other datasets may fit so we continue the loop
                    continue

            
            #other case name: is when the regex is a fully specified  scheme.table and  obeys a template
            if singleEntry and  re.search("TEMPLATE",name):
                #reverse the search: turn the TEMPLATE into a regex and assume the regex is a fully qualified name for the class 
                srch=re.sub("TEMPLATE","([^\\\s]+)",name.replace(".","\."))
                if re.search(srch,regex):
                    #we really should only land in this code area when no outdset has been defined yet
                    assert not outdsets
                    #dynamically derive a class from the  template
                    if regex in self.__dscache__:
                        ds=self.__dscache__[regex]
                    else:
                        dsbase=self.getDsetClass(conf,name)
                        scheme,tbl=regex.split(".")
                        #derive a new class from the template class
                        ds=type(tbl,(dsbase,),{"scheme":scheme})
                    outdsets=[ds]
        
        if not outdsets and singleEntry:
            #fall back with a generic type

            scheme,tbl=regex.split(".")
            # dsgen=self.getDsetClass(conf,"anyscheme.DataSetGeneric")
            outdsets.append(type(tbl,(DataSetGeneric,),{"scheme":scheme}))

        return outdsets
        
    def getDFuncClass(self,conf,name):
        """Loads a database function as an class (but check cache first)"""
        if name in self.__dfcache__:
            return self.__dfcache__[name]
        else:
           self.loadCatalogue(conf) 
           dfentry=self.__catalogue__["functions"][name]
           #load isolated class
           mod=import_module(dfentry["module"])
           df=getattr(mod,name.split(".")[1])
           self.__dfcache__[name]=df
           return df



    def getFuncs(self,conf,regex):
        """retrieves a list of database functions possibly obeying a certain regex"""
        self.loadCatalogue(conf)        
        #get the valid names  
        outdfs=[] 
        regexcomp=re.compile(regex)
        #we expect to have only one matching entry when the regex is a fully-qualified scheme.function name
        singleEntry=re.fullmatch("^[\w]+(?:\.[\w]+)$",regex)
        for name in self.__catalogue__["functions"].keys():
            df=None
            if regexcomp.fullmatch(name):
                df=self.getDFuncClass(conf,name)
                if singleEntry:
                    if not outdfs:
                        outdfs=[None]
                    #this possibly overwrites a fitting template class
                    outdfs[0]=df
                    #no need to look any further we found our fit
                    break
                else:
                    outdf.append(df)
                    #note: other functions may fit so we continue the loop
                    continue

        return outdfs

    def getViewClass(self,conf,name):
        """Loads a database view as an class (but check cache first)"""
        if name in self.__dvcache__:
            return self.__dvcache__[name]
        else:
           self.loadCatalogue(conf) 
           dventry=self.__catalogue__["views"][name]
           #load isolated class
           mod=import_module(dventry["module"])
           dv=getattr(mod,name.split(".")[1])
           self.__dvcache__[name]=dv
           return dv



    def getViews(self,conf,regex):
        """retrieves a list of database views possibly obeying a certain regex"""
        self.loadCatalogue(conf)        
        #get the valid names  
        outdvs=[] 
        regexcomp=re.compile(regex)
        #we expect to have only one matching entry when the regex is a fully-qualified scheme.function name
        singleEntry=re.fullmatch("^[\w]+(?:\.[\w]+)$",regex)

        for name in self.__catalogue__["views"].keys():
            dv=None
            if regexcomp.fullmatch(name):
                dv=self.getViewClass(conf,name)
                if singleEntry:
                    if not outdvs:
                        outdvs=[None]
                    #this possibly overwrites a fitting template class
                    outdvs[0]=dv
                    #no need to look any further we found our fit
                    break
                else:
                    outdvs.append(dv)
                    #note: other functions may fit so we continue the loop
                    continue

        return outdvs

#module wide variable which allows registration of dataset classes, functions, datasetfactories and views
geoslurpCatalogue=DatasetCatalogue()




