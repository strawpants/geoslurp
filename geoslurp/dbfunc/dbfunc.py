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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019

from abc import ABC, abstractmethod
import os
from geoslurp.config.slurplogger import slurplogger
from geoslurp.db import Inventory,Settings

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import text
from sqlalchemy.sql import func
from datetime import datetime

class DBFunc(ABC):
    """Abstract Base class which holds a database functione"""
    scheme='public'
    db=None
    version=(0,0,0)
    updatefreq=None
    inargs="" # or something like "integer,varchar"
    outargs=None #output of the function (e.g. "TABLE(int,varchar)")
    pgbody="" # contains the PGSQL body code
    language='plpgsql'
    def __init__(self,dbcon):
        self.name=self.__class__.__name__.lower().replace('-',"_")
        self.db=dbcon

        #Initiate a session for keeping track of the inventory entry
        self._ses=self.db.Session()
        invent=Inventory(self.db)
        try:
            self._dbinvent=self._ses.query(invent.table).filter(invent.table.scheme == self.scheme ).filter(invent.table.pgfunc == self.name).one()
        except NoResultFound:
            #possibly create a schema
            self.db.CreateSchema(self.scheme)
            #set defaults for the  inventory
            self._dbinvent = invent.table(scheme=self.scheme, pgfunc=self.name,
                    version=self.version, updatefreq=self.updatefreq,data={}, 
                    lastupdate=datetime.min, owner=self.db.user)
            #add the default entry to the database
            self._ses.add(self._dbinvent)
            self._ses.commit()
        #load user settings
        self.conf=Settings(self.db)

    def updateInvent(self,updateTime=True):
        if updateTime:
            self._dbinvent.lastupdate=datetime.now()
        self._dbinvent.updatefreq=self.updatefreq
        self._ses.commit()

    def info(self):
        return self._dbinvent

    def purgeentry(self):
        """Delete pgfunction entry in the database"""
        slurplogger().info("Deleting %s function entry"%(self.name))
        self._ses.delete(self._dbinvent)
        self._ses.commit()
        
        dropexec=text("DROP FUNCTION IF EXISTS %s.%s;"%(self.scheme,self.name))
        self.db.dbeng.execute(dropexec) 
    def register(self):
        #iterate over overloaded functions
        if type(self.inargs) == list and type(self.pgbody) == list:
            #iterate over mutile function overloads
            for inargs,pgbody in zip(self.inargs,self.pgbody):
                self.register_overload(inargs,pgbody)
        else:
                self.register_overload(self.inargs,self.pgbody)

        self.updateInvent()
    

    def register_overload(self,inargs,pgbody):
        """Creates/replaces the (overloaded) pgfunction in the database"""
        slurplogger().info("Registering (overload) of %s function"%(self.name))
        pgheader="CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s AS $dbff$\n"%(self.name,inargs,self.outargs)
        pgfooter=";\n$dbff$ LANGUAGE %s;"%(self.language)
        # print(str(pgheader+pgbody+pgfooter))
        self.db.dbeng.execute(text(pgheader+pgbody+pgfooter)) 
