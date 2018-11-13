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

import os.path
import yaml
import sys
from collections import namedtuple
import re

def getCreateDir(returndir):
    """creates a directory when not existent and return it"""
    if not os.path.exists(returndir):
        os.makedirs(returndir)
    return returndir

Credentials=namedtuple("Credentials","user passw alias")

def findFiles(dir,pattern):
    """Generator to recursively search adirecctor (returns a generator)"""
    for dpath,dnames,files in os.walk(dir):
        # for subdir in dnames:
        #     yield from findFiles(os.path.join(dir,subdir),pattern)

        for file in files:
            if re.search(pattern,file):
                yield os.path.join(dpath,file)

class SlurpConf:
    """ Class which reads and writes configure data in yaml format and contains a database connector"""
    def __init__(self, conffile):
        self.confyaml=conffile
        self._confDict=self.read(self.confyaml)

    def printDefault(self,out=sys.stderr):
        """Write the default configuration """
        obj={"dburl":"postgresql+psycopg2://geoslurp:Swapwithyour0wn@localhost/geoslurp","DataDir":"/tmp/geoslurp/data","CacheDir":"/tmp/geoslurp/cache","PluginDir":"/tmp/geoslurp/plugins"}
        out.write("# Default configuration file for geoslurp\n")
        out.write("# Change settings below and save file to .geoslurp.yaml\n")
        yaml.dump(obj,out,default_flow_style=False)

    def write(self,conffile=None):
        """Writes changed setup back to the yaml configuration file"""
        if not conffile:
            conffile=self.confyaml
        with open(conffile,'w') as fid:
            yaml.dump(self._confDict, fid, default_flow_style=False)

    def read(self,conffile):
        """Read the parameters from the yaml onfiguration file"""
        if os.path.exists(conffile):
            #Read parameters from yaml file
            with open(conffile, 'r') as fid:
                return yaml.safe_load(fid)
        else:
            raise Exception('cannot find geoslurp configuration file')

    def authCred(self,service):
        """obtains username credentials for a certain service
        :param service: name of the service
        :returns a namedtuple with credentials"""
        return Credentials(alias=service, **self._confDict["Auth"][service])

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self, key):
        return self._confDict[key]

    def __setitem__(self, key, val):
       self._confDict[key]=val

    def getDir(self,scheme, dirEntry, dataset=None,subdirs=None):
        """
        :param scheme str: name of the database scheme
        :param dataset str: name of the dataset
        :param dirEntry: type of the directory to look for (CacheDir, or DataDir)
        :return: the directory (possibly created when it doesn't exist)
        """
        #begin with setting the default
        dirpath=getCreateDir(os.path.join(self._confDict[dirEntry],scheme))

        #let's see if there is a specialized 'DataDir' entry for the dataset
        if dataset:
            try:
                dsetpath=getCreateDir(self._confDict[scheme][dataset][dirEntry])
                #upon success let's add a symbolic link in the scheme datadir
                try:
                    os.symlink(dsetpath,os.path.join(dirpath,dataset))
                except:
                    #ok when it already exists
                    pass
                dirpath=dsetpath
            except KeyError:
                # no problem we can just stick with the default
                dirpath=getCreateDir(os.path.join(dirpath,dataset))

        #possibly append subdirectories
        if subdirs:
            dirpath=getCreateDir(os.path.join(dirpath,subdirs))

        return dirpath

