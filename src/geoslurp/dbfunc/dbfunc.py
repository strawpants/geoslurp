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
import re
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import text
from sqlalchemy.sql import func
from datetime import datetime
import inspect

class DBFunc(ABC):
    """Abstract Base class which holds a database functione"""
    schema='public'
    db=None
    version=(0,0,0)
    updatefreq=None
    inargs="" # or something like "integer,varchar"
    outargs=None #output of the function (e.g. "TABLE(int,varchar)")
    pgbody="" # contains the PGSQL body code
    language='plpgsql'
    
    @classmethod
    def sfname(cls):
        return cls.schema+"."+cls.__name__.lower().replace("-","_")
    
    @classmethod
    def fname(cls):
        return cls.__name__.lower().replace("-","_")
    
    def __init__(self,dbcon):
        self.name=self.fname()
        self.db=dbcon

        #Initiate a session for keeping track of the inventory entry
        self._ses=self.db.Session()
        invent=Inventory(self.db,ses=self._ses)
        try:
            self._dbinvent=self._ses.query(invent.table).filter(invent.table.scheme == self.schema ).filter(invent.table.pgfunc == self.name).one()
        except NoResultFound:
            #possibly create a schema
            self.db.CreateSchema(self.schema)
            #set defaults for the  inventory
            self._dbinvent = invent.table(scheme=self.schema, pgfunc=self.name,
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
        self._ses.delete(self._dbinvent)
        #extract the argument types
        if type(self.inargs) == list:
            iargs=self.inargs
        else:
            iargs=[self.inargs]

        for iarg in iargs:
            saniarg=re.sub(r'\s*,\s+',',',iarg).split(",")
            atypes=",".join([re.search(r'\S+\s+([a-zA-Z0-9]+)[,=$]?',sa).group(1) for sa in saniarg])
            dropexec=text(f"DROP FUNCTION IF EXISTS {self.schema}.{self.name}({atypes})")
            slurplogger().info(f"Deleting {dropexec.text} function entry")
            self._ses.execute(dropexec) 
        self._ses.commit()
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
        self._ses.execute(text(pgheader+inspect.cleandoc(pgbody)+pgfooter)) 
        self._ses.commit()
