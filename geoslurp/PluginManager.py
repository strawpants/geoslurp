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
from glob import glob
from .slurpconf import slurpconf  
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
                    cl=loadPlugin(pyf)
                    tag=type(cl).__name__
                    self.plugins[tag]=cl
                    self.pluginFiles[tag]=pyf
        self.lastcheck
    
    
    def __init__(self):
        #load settings
        self.conf=slurpconf()
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


    def list(self,out=sys.stderr):
        """List loaded plugins"""
        #create a list of potential plugins
        for key,cl in self.plugins.items():
            print(type(cl).__name__,": ",type(cl).__doc__,file=out)

    
def loadPlugin(path):
    """Loads the plugin class from python file (path)"""
    #note this is python version specific code
    if sys.version_info < (3,5,0) and sys.version_info > (3,3,0):
        from importlib.machinery import SourceFileLoader
        mod = SourceFileLoader("geoslurp.plugins",path).load_module() 
        return mod.PlugName()
    elif sys.version_info >= (3,5,0):
        spec = importlib.util.spec_from_file_location("geoslurp.plugins", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.PlugName()
    else:
        raise Exception('This python version cannot dynamically load a plugin')
