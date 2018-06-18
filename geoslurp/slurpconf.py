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

def getCreateDir(returndir):
    """creates a directory when not existent and return it"""
    if not os.path.exists(returndir):
        os.makedirs(returndir)
    return returndir

Log=sys.stdout

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
        yaml.dump(self.confobj,fid,default_flow_style=False)
        fid.close()

    def read(self,conffile):
        """Read the parameters from the yaml onfiguration file"""
        if os.path.exists(conffile):
            #Read parameters from yaml file
            fid=open(conffile,'r')
            self.confobj=yaml.safe_load(fid)
            fid.close()
        else:
            raise Exception('cannot find geoslurp configuration file')

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self,arg):
        return self.confobj[arg]

    #def __setitem__(self,key,val):
     #   self.confobj[key]=val

    # def getDataDir(self,datasource):
        # """Retrieves the data directory by appending a subdir and creates it when not existent"""
        # #default path unless overwritten
        # ddir=os.path.join(self.confobj['DataDir'],datasource)
        
        # #first try to retrieve the datasource specific datadirectory
        # if datasource in self.confobj:
            # if 
            # ddir=self.confobj[datasource]['DataDir'] 
        # else:
            # self.confobj[datasource]={}
            # self.confobj[datasource]['DataDir']=ddir
        
        # try:
            # return getCreateDir(self.confobj[datasource]['DataDir'])
        # except KeyError as err:
            # #else we use the default path
            # if datasource in self.confobj:
                # self.confobj[datasource]['DataDir']=os.path.join(self.confobj['DataDir'],datasource)
            # else:
                # self.confobj[datasource]={}
                # self.confobj[datasource]['DataDir']=
            # return getCreateDir(self.confobj[datasource]['DataDir'])
    
    def getDataSource(self,datasource):
        """initializes datasource structure from file or to default"""
        if not datasource in self.confobj:
           self.confobj[datasource]={}
        
        if not 'DataDir' in self.confobj[datasource]:
            #create default data directory
            self.confobj[datasource]['DataDir']=getCreateDir(os.path.join(self.confobj['DataDir'],datasource))
        else:
            getCreateDir(self.confobj[datasource]['DataDir'])
        
        if not 'CacheDir' in self.confobj[datasource]:
            #create default data directory
            self.confobj[datasource]['CacheDir']=getCreateDir(os.path.join(self.confobj['CacheDir'],datasource))
        else:
            getCreateDir(self.confobj[datasource]['CacheDir'])
        
        return self.confobj[datasource]

    # def getCacheDir(self,subdir):
        # """Retrieves the cache directory by appending a subdir and creates it when not existent"""
        # return getCreateDir(os.path.join(self.confobj['CacheDir'],subdir))
    
    def setLogger(self):
        """set where to output log info"""
        try:
            logfile=self.confobj['Logger']
            Log=open(logfile,'w')
        except:
            Log=sys.stdout
