#!/usr/bin/python3
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

#this is intended to be turned into a daemon services at some stage
import  importlib.util
import sys
import os.path,datetime
import shutil
from glob import glob

class PluginManager():
    """ Manages plugins which can be loaded and executed dynamically"""
    def checkForPlugins(self):
        """Dynamically updates the loaded plugins"""
        
        #Check for possible deleted plugins
        for key,val in self.pluginFiles.items():
            if not os.path.exists(val):
                del self.plugins[key]
                del self.pluginFiles[key]
        
        for pdirec in self.pluginpath:
            for pyf in glob(os.path.join(pdirec,'')+'*.py'):
                #check if file is unchanged and already loaded
                tmod= datetime.datetime.fromtimestamp(os.path.getmtime(pyf))
                if tmod > self.lastcheck:
                    #reload plugin
                    cl=self.loadPlugin(pyf)
                    tag=type(cl).__name__
                    self.plugins[tag]=cl
                    self.pluginFiles[tag]=pyf
        self.lastcheck

    def __getitem__(self,clname):
        """Forward the [] call to retrieve the registered plugin"""
        return self.plugins[clname]
    
    def __init__(self,conf):
        #load settings
        self.conf=conf
        self.pluginpath=[os.path.join(os.path.dirname(__file__),'plugins')]
        try:
            self.pluginpath.append(self.conf['PluginDir'])
        except KeyError:
            pass
        #enforces loading of all plugins upons first use
        self.lastcheck=datetime.datetime.min
        self.plugins={}
        self.pluginFiles={}
        self.checkForPlugins()
        self.verbose=0
    
    def verbose(self):
        self.verbose=1

    # def list(self,out=sys.stderr):
        # """List available plugins and status"""
        # #create a list of potential plugins
        # for key,cl in self.plugins.items():
            # print(type(cl).__name__,": ",type(cl).__doc__,file=out)

    
    def loadPlugin(self,path):
        """Loads the plugin class from python file (path)"""
        #note this is python version specific code
        if sys.version_info < (3,5,0) and sys.version_info > (3,3,0):
            from importlib.machinery import SourceFileLoader
            mod = SourceFileLoader("geoslurp.plugins",path).load_module() 
            return mod.PlugName(self.conf)
        elif sys.version_info >= (3,5,0):
            spec = importlib.util.spec_from_file_location("geoslurp.plugins", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod.PlugName(self.conf)
        else:
            raise Exception('This python version cannot dynamically load a plugin')
    
    def cleancache(self):
        for f in  os.listdir(self.conf['CacheDir']):
            filep=os.path.join(self.conf['CacheDir'],f)
            if os.path.isfile(f):
                #print(filep)
                os.unlink(filep)
            elif os.path.isdir(filep):
                shutil.rmtree(filep)
    
    def printconfig(self):
        self.conf.default(sys.stdout)
    
    def addArgs(self,parser):
        """Add command line arguments (also for the loaded plugins)"""
        
        parser.add_argument('-u','--update',action='store_true',help="update selected datasources")
        parser.add_argument('-r','--remove',action='store_true',help="remove selected datasource files and database entries")
        parser.add_argument('--printconfig',action='store_true',help='Prints out a default configuration file (default file is ~/.geoslurp.yaml)')
        parser.add_argument('--cleancache',action='store_true',help="Clean up the cache directory")
        parser.add_argument('--force',action='store_true',help='enforce action')
        parser.add_argument('--verbose',action='store_true',help='Be more verbose')
        
        #also add datasource options
        subparsers = parser.add_subparsers(help='Datasource to select',dest='datasource') 

        for key,cl in self.plugins.items():
            cl.addParserArgs(subparsers)
    
    def execTasks(self,args):
        """execute tasks contained within the arguments"""
        if args.printconfig:
            self.printconfig()
            sys.exit(0)
        
        if args.cleancache:
            self.cleancache()
        
        if not 'datasource' in args:
            sys.exit(0)

        if args.update:
            self.plugins[args.datasource].update(args.force)
        
        if args.remove:
            self.plugins[args.datasource].remove()


