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
# from  geoslurp.db import GeoslurpConnector

from geoslurp.schema import Schema
import os, shutil



class SchemaManager():
    """Manages and schedules geoslurp tasks"""
    def __init__(self):
        """Gathers all the necessary components to have a working geoslurp instance"""
        
       #read configuration stuff
        # self.conf=slurpconf(conffile)
        #set up a database connector 
        # self.db=GeoslurpConnector(self.conf["dburl"])
        #put all (implemented classes = derived classes ina dictionary)
        # Note that we do NOT initiate the classes here
        self._schemeClasses=dict( [ (x.__name__,x) for x in Schema.__subclasses__()])

    def items(self):
        for x,v in self._schemeClasses.items():
            yield x,v
    # def execTasks(self,args):
    #     """execute tasks as requested by the arguments"""
    #     if args.printconfig:
    #         self.conf.printDefault()
    #
    #     if args.cleancache:
    #         if args.datasource:
    #             self.cleancache(args.datasource)
    #         else:
    #             self.cleancache()
    #
    #     if args.list:
    #         #retrieve the entries from the inventory table
    #         ses=self.db.Session()
    #         dbinvent=ses.query(Invent).options(load_only('datasource','lastupdate'))
    #         for ds in dbinvent:
    #             print("Datasource:",ds.datasource,", last updated:",ds.lastupdate)
    #     if not args.datasource:
    #         return
    #
    #     if args.purge:
    #         self.purge(args.datasource)
    #         return
    #     #iextract datasource specific configuration
    #     confds=self.conf.getDataSource(args.datasource)
    #
    #     #if we land here we pass the arguments to the plugin calss itself
    #     self.plugman.plugins[args.datasource](self.db,confds).parseAndExec(args)
    #
    #
    # def cleancache(self,subdir=None):
    #     """cleans the Cache directory entirely"""
    #
    #     if subdir == None:
    #         dirn=self.conf['CacheDir']
    #     else:
    #         dirn=os.path.join(self.conf['CacheDir'],subdir)
    #
    #     for f in  os.listdir(dirn):
    #         filep=os.path.join(dirn,f)
    #         if os.path.isfile(f):
    #             #print(filep)
    #             os.unlink(filep)
    #         elif os.path.isdir(filep):
    #             shutil.rmtree(filep)
    #
    #
    # def purge(self,datasource):
    #     """purge selected datasource (db tables and data files)"""
    #     #remove the datadir and it's content
    #     try:
    #         import pdb;pdb.set_trace()
    #         ddir=self.conf.getDataSource(datasource)['DataDir']
    #         if os.path.isdir(ddir):
    #             shutil.rmtree(ddir)
    #     except KeyError:
    #         pass
    #
    #     #now also remove the scheme and all tables/indexes
    #     try:
    #         self.db.dropSchema(datasource,cascade=True)
    #     except:
    #         pass
    #     #also  try to remove entry in the inventory
    #     ses=self.db.Session()
    #     ses.query(Invent).filter(Invent.datasource == datasource).delete()
    #     ses.commit()
    #     ses.close()

        
