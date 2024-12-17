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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2020

from geoslurp.config.slurplogger import slurplogger
from geoslurp.db import Inventory,Settings
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime

class TView:
    """Base class which holds and manages a table view"""
    sqlqry=None
    schema='public'
    db=None
    version=(0,0,0)
    
    @classmethod
    def svname(cls):
        return cls.schema+"."+cls.__name__.lower().replace("-","_")
    
    @classmethod
    def vname(cls):
        return cls.__name__.lower().replace("-","_")
    
    def __init__(self,dbcon):
        self.name=self.vname()
        self.db=dbcon

        #Initiate a session for keeping track of the inventory entry
        self._ses=self.db.Session()
        invent=Inventory(self.db,ses=self._ses)
        try:
            self._dbinvent=self._ses.query(invent.table).filter(invent.table.scheme == self.schema ).filter(invent.table.view == self.name).one()
        except NoResultFound:
            #possibly create a schema
            self.db.CreateSchema(self.schema)
            #set defaults for the  inventory
            self._dbinvent = invent.table(scheme=self.schema, view=self.name,
                    version=self.version, data={}, lastupdate=datetime.min, owner=self.db.user)
            #add the default entry to the database
            self._ses.add(self._dbinvent)
            self._ses.commit()
        #load user settings
        self.conf=Settings(self.db)
        
    def updateInvent(self,updateTime=True):
        if updateTime:
            self._dbinvent.lastupdate=datetime.now()
        self._ses.commit()

    def info(self):
        return self._dbinvent

    def register(self):
        """Register the view in the database"""
        slurplogger().info("Creating view %s "%(self.name))
        self.db.dropView(self.name,schema=self.schema)
        self.db.createView(self.name,schema=self.schema,qry=self.qry)

    def purgeentry(self):
        """Delete table view entry in the database"""
        slurplogger().info("Deleting %s entry"%(self.name))
        self._ses.delete(self._dbinvent)
        self._ses.commit()
        self.db.dropView(self.name,self.schema)

    def halt(self):
        """can be overridden to properly clean up an aborted operation"""
        pass

    def exists(self):
        return self.db.tableExists(self.svname())
