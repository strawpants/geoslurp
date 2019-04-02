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

class DatasetRegister:
    #holds dataset classes (not initiated!)
    __dsets__=[]
    # holds factory methods for dyanamically building datasets
    __dsetfac__=[]
    def __init__(self):
        pass

    def registerDataset(self,datasetcls):
        self.__dsets__.append(datasetcls)
    
    def registerDatasetFactory(self,datasetclsfac):
        self.__dsetfac__.append(datasetclsfac)

    def load(self,conf):
        """Loads all datasets from the factories and appends them to the __dsets__"""
        if not self.__dsetfac__:
            #Quick return when nothing needs to be done
            return

        for fac in self.__dsetfac__:
           self.__dsets__.extend(fac(conf)) 
        #set the factory list to an empty list (everything has been produced)
        self.__dsetfac__=[]

    def getDatasets(self,conf,regex=None):
        """retrieves a list of dataset classes possibly obeying a certain regex"""
        if conf:
            #also load the dynamic factory-based datasets
            self.load(conf)
        
        if not regex:
            #just return everything
            return self.__dsets__

        #otherwise be more picky
        outdsets=[]
        regexcomp=re.compile(regex)
        for ds in self.__dsets__:
            if regexcomp.search(ds.__name__):
                outdsets.append(ds)
        return outdsets

    def getFuncs(self,conf):
        """This is currently a stub (returns no functions)"""
        return []
#module wide variable which allows registration of dataset classes and datasetfactories (dynamic reguires )
geoslurpregistry=DatasetRegister()


