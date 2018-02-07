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
import os.path
from glob import glob
from .slurpconf import slurpconf  
class PluginManager():
    """ Manages plugins which can be loaded and executed dynamically"""
    def __init__(self):
        #load settings
        self.conf=slurpconf()
        self.pluginpath=[os.path.join(os.path.dirname(__file__),'plugins')]
        try:
            self.pluginpath.append(self.conf['PluginDir'])
        except KeyError:
            pass

    def list(self,out=sys.stderr):
        """List available plugins"""
        #create a list of potential plugins
        for pdirec in self.pluginpath:
            for pyf in glob(os.path.join(pdirec,'')+'*.py'):
                #dynamically load class and print class name and doc string
                cl=loadPlugin(pyf)
                print(type(cl).__name__,": ",cl.__doc__,file=out)

    
def loadPlugin(path):
    """Loads the plugin class from python file (path)"""
    #note this is python version specific code
    if sys.version_info < (3,5,0) and sys.version_info > (3,3,0):
        from importlib.machinery import SourceFileLoader
        mod = SourceFileLoader("geoslurp.plugins",path).load_module() 
        return mod.PlugName()
    else:
        raise Exception('This python version cannot dynamically load a plugin')
