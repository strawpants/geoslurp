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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2024


from geoslurp.config.localsettings import readLocalSettings,settingsArgs
from geoslurp.db.connector import GeoslurpConnector
from geoslurp.db import Settings
from geoslurp.config.catalogue import DatasetCatalogue
from sqlalchemy import text

class GeoslurpManager:
    def __init__(self,args=None,readonly_user=True,conf_file=None,dbalias=None):
        #if no overruling arguments are supplied
        if not args:
            #Start with the default arguments
            args=settingsArgs()

        if conf_file is not None:
            #read the local client settings from non-default yaml config file
            args.local_settings=conf_file
        
        if dbalias:
            #specify a different database alias profile to load from the in the client settings
            args.dbalias=dbalias

        
        #Read local settings 
        self._localconf=readLocalSettings(args=args,readonlyuser=readonly_user)
        self._catalogue=DatasetCatalogue()
        #note no actual database connection is needed up to now
        self._conn=None
        self._conf=None #settings loaded from the database
        
    @property
    def conn(self)-> GeoslurpConnector :
        """
        Lazy loading of the geoslurp connector
        Returns
        -------
        GeoslurpConnector
        """
        if self._conn is None:
            #initialize connection based on the client profile
            self._conn=GeoslurpConnector(host=self._localconf.host,user=self._localconf.user,passwd=self._localconf.password, cache=self._localconf.cache,dataroot=self._localconf.dataroot)
        
        
        return self._conn
    
    @property
    def conf(self):
        """
            Lazily loads the user/db configuration from the database
        """
        if self._conf is None:
            self._conf=Settings(self.conn)
        return self._conf
    
    def execute(self,qry):
        return self.conn.execute(qry)
    
    def dataset(self,name:str):
        
        #initialize a dataset class (Note this requires a database connection)
        ds=self._catalogue.getDsetClass(self.conf,name)
        #return an initialized dataset class
        return ds(self.conn)

    @property
    def clientconf(self):
        return self._localconf
