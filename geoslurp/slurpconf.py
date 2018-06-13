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
from .geoslurpClient import geoslurpClient
from .PluginManager import PluginManager

def getCreateDir(root,subdir):
    """creates a directory when not existent and return it"""
    returndir=os.path.join(root,subdir)
    if not os.path.exists(returndir):
        os.makedirs(returndir)
    return returndir


class slurpconf():
    """ Class which reads and writes configure data in yaml format and contains a database connector"""
    def __init__(self,conffile):
        self.confyaml=conffile
        self.read(self.confyaml)
        self.setLogger()

    def printDefault(self,out=sys.stderr):
        """Write the default configuration """
        obj={"dburl":"postgresql+psycopg2://geoslurp:Swapwithyour0wn@localhost/geoslurp","DataDir":"/tmp/geoslurp/data","CacheDir":"/tmp/geoslurp/cache","PluginDir":"/tmp/geoslurp/plugins"}
        out.write("# Default configuration file for geoslurp\n")
        out.write("# Change settings below and save file to .geoslurp.yaml\n")
        yaml.dump(obj,out,default_flow_style=False)

    def write(self,conffile):
        """Writes changed setup back to the yaml configuration file"""
        fid=open(self.conffile,'w')
        yaml.dump(self.confobj[0],fid,default_flow_style=False)
        fid.close()

    def read(self,conffile):
        """Read the parameters from the yaml onfiguration file"""
        if os.path.exists(conffile):
            #Read parameters from yaml file
            fid=open(conffile,'r')
            self.confobj=[x for x in yaml.safe_load_all(fid)]
            fid.close()
        else:
            raise Exception('cannot find geoslurp configuration file')

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self,arg):
        return self.confobj[0][arg]

    #def __setitem__(self,key,val):
     #   self.confobj[0][key]=val

    def getDataDir(self,subdir):
        """Retrieves the data directory by appending a subdir and creates it when not existent"""
        return getCreateDir(self.confobj[0]['DataDir'],subdir)

    def getCacheDir(self,subdir):
        """Retrieves the cache directory by appending a subdir and creates it when not existent"""
        return getCreateDir(self.confobj[0]['CacheDir'],subdir)
    
    def setLogger(self):
        """set where to output log info"""
        try:
            logfile=self.confobj[0]['Logger']
            self.log=open(logfile,'w')
        except:
            self.log=sys.stdout
