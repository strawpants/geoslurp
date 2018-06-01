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

from .slurpconf import slurpconf
from .geoslurpClient import geoslurpClient
from .PluginManager import PluginManager
class taskScheduler():
    """Manages and schedules geoslurp tasks"""
    def __init__(self,conffile):
        """Gathers all the necessary components to have a working geoslurp instance"""
        
       #read configuration stuff
        self.conf=slurpconf(conffile)
        #set up a database connector 
        self.db=geoslurpClient(self.conf["dburl"])

        #load available plugins
        self.plugman=PluginManager(self.conf['PluginDir'])
        
    
    def addArgs(self,parser):
        """Add command line arguments (also for the loaded plugins)"""
        
        parser.add_argument('-r','--remove',action='store_true',help="remove all datasets and database entries of the selected datasource")
        parser.add_argument('--printconfig',action='store_true',help='Prints out a default configuration file (default file is ~/.geoslurp.yaml)')
        parser.add_argument('--cleancache',action='store_true',help="Clean up the cache directory")
        parser.add_argument('--force',action='store_true',help='enforce action')
        parser.add_argument('--verbose',action='store_true',help='Be more verbose')
        
        #also add datasource options
        subparsers = parser.add_subparsers(help='Datasource to select',dest='datasource') 

        for key,cl in self.plugman.plugins.items():
            cl.addParserArgs(subparsers)
    
    
    def execTasks(self,args):
        """execute tasks as requested by the arguments"""
        if args.printconfig:
            self.conf.printDefault()
        
        if args.cleancache:
            self.cleancache()
        
        if not 'datasource' in args:
            return

        self.plugman.plugins[args.datasource](self.db,self.conf).parseAndExec(args)


    def cleancache(self):
        """cleans the Cache directory entirely"""
        for f in  os.listdir(self.conf['CacheDir']):
            filep=os.path.join(self.conf['CacheDir'],f)
            if os.path.isfile(f):
                #print(filep)
                os.unlink(filep)
            elif os.path.isdir(filep):
                shutil.rmtree(filep)
