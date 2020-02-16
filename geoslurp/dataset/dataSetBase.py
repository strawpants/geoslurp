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

from abc import ABC, abstractmethod
import os
from geoslurp.config.slurplogger import slurplogger
import shutil
import re
from geoslurp.db import Inventory,Settings
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime
from sqlalchemy import Table,Column,Integer,String
from sqlalchemy.dialects.postgresql import TIMESTAMP
from geoslurp.datapull import UriFile
from sqlalchemy import and_
from geoslurp.db.settings import getCreateDir
from geoslurp.db import tableMapFactory


def rmfilterdir(ddir,filter='*'):
    """Remove directories and files based on a certain regex filter"""
    if filter == '*':
        if os.path.isdir(ddir):
            slurplogger().info("Pruning directory %s"%(ddir))
            shutil.rmtree(ddir)
    else:
        #remove partly
        for root, dirs, files in os.walk(ddir):
            #possibly remove entire directories with content at once
            for dd in dirs:
                if re.search(filter,dd):
                    slurplogger().info("Pruning directory %s"%(dd))
                    shutil.rmtree(dd)
                    #also remove from the file tree so it won't be followed
                    dirs.remove(dd)
            #remove filenames
            for fl in files:
                if re.search(filter,fl):
                    file=os.path.join(ddir,root,fl)
                    slurplogger().info("Removing %s"%(file))
                    os.remove(file)

class DataSet(ABC):
    """Abstract Base class which hold a dataset (corresponding to a database table"""
    table=None
    commitCounter=0
    scheme='public'
    db=None
    version=(0,0,0)
    updatefreq=None
    commitperN=500
    stripuri=False
    def __init__(self,dbcon):
        if re.search("TEMPLATE",".".join([self.scheme,self.__class__.__name__])):
            raise RuntimeError("Refusing to instantiate templated dataset")

        self.name=self.__class__.__name__.lower().replace('-',"_")
        self.db=dbcon

        #Initiate a session for keeping track of the inventory entry
        self._ses=self.db.Session()
        invent=Inventory(self.db)
        try:
            self._dbinvent=self._ses.query(invent.table).filter(invent.table.scheme == self.scheme ).filter(invent.table.dataset == self.name).one()
            #possibly migrate table
            self.migrate(self._dbinvent.version)
        except NoResultFound:
            #possibly create a schema
            self.db.CreateSchema(self.scheme)
            #set defaults for the  inventory
            self._dbinvent = invent.table(scheme=self.scheme, dataset=self.name,
                    version=self.version, updatefreq=self.updatefreq,data={}, 
                    lastupdate=datetime.min, owner=self.db.user)
            #add the default entry to the database
            self._ses.add(self._dbinvent)
            self._ses.commit()
        #load user settings
        self.conf=Settings(self.db)
        
        #possibly create the table when explictily provided
        # the table creation will be postponed when no explicit table is provided
        if self.table:
            self.createTable()


    def updateInvent(self,updateTime=True):
        if updateTime:
            self._dbinvent.lastupdate=datetime.now()
        self._dbinvent.updatefreq=self.updatefreq
        self._ses.commit()

    def info(self):
        return self._dbinvent

    def dataDir(self,subdirs=None):
        """Returns the specialized data directory of this scheme and dataset
        The directory will be created if it does not exist"""
        if self._dbinvent.datadir:
            return getCreateDir(self._dbinvent.datadir,self.conf.mirrorMap)
        #else try to retrieve the standard datadir from the configuration
        return self.conf.getDataDir(self.scheme, dataset=self.name,subdirs=subdirs)
    
    def setDataDir(self,ddir):
        self._dbinvent.datadir=ddir
        self.updateInvent(False)

    def cacheDir(self,subdirs=None):
        """returns the cache directory of this scheme and dataset"""
        if self._dbinvent.cache:
            if subdirs:
                return getCreateDir(os.path.join(self._dbinvent.cache,subdirs),self.conf.mirrorMap)
            else:
                return getCreateDir(self._dbinvent.cache,self.conf.mirrorMap)

        return self.conf.getCacheDir(self.scheme, dataset=self.name,subdirs=subdirs)
    
    def setCacheDir(self,cdir):
        self._dbinvent.cache=cdir
        self.updateInvent(False)

    @abstractmethod
    def pull(self):
        """Pulls the necessary data from the online resource"""
        pass

    @abstractmethod
    def register(self):
        """Register the downloaded dataset in the database"""
        pass

    def purgedata(self,filter='*'):
        """Deletes the data directory of the dataset,optionally applying a directory/filename filter"""
        rmfilterdir(self.dataDir(),filter)

    def purgecache(self,filter='*'):
        """Deletes the cache directory of the dataset, optionally applying a directory/filename filter"""
        rmfilterdir(self.cacheDir(),filter)

    def purgeentry(self):
        """Delete dataset entry in the database"""
        slurplogger().info("Deleting %s entry"%(self.name))
        self._ses.delete(self._dbinvent)
        self._ses.commit()
        self.db.dropTable(self.name,self.scheme)

    def halt(self):
        """can be overridden to properly clean up an aborted operation"""
        pass

    def uriNeedsUpdate(self, urilikestr,lastmod):
        """Query for a URI in the table based on a alike string and delete the entry when older than lastmod"""
        needsupdate=True
        try:
            qResults=self._ses.query(self.table).filter(self.table.uri.like('%'+urilikestr+'%'))
            if qResults.count() == 0:
                return True
            needsupdate=False
            #check if at least one needs updating
            for qres in qResults:
                if qres.lastupdate < lastmod:
                    needsupdate=True
                    break

            if needsupdate:
                for qres in qResults:
                    #delete the entries which need updating
                    self._ses.delete(qres)
                    self._ses.commit()
            else:
                slurplogger().info("No Update needed, skipping %s"%(urilikestr))

        except Exception as e:
            # Fine no entries found
            pass
        return needsupdate

    def retainnewUris(self,urilist):
        """Filters those uris which have table entries which are too old or are not present in the database"""
        #create a temporary table with uri and lastmodification time entries
        cols=[Column('id', Integer, primary_key=True),Column('uri',String),Column('lastmod',TIMESTAMP)]

        #setup a seperate session  and transaction in order to work with a temporary table
        trans,ses=self.db.transsession()
        
        tmptable=self.db.createTable('tmpuris',cols,temporary=True,bind=ses.get_bind())
        # tmptable=self.db.createTable('tmpuris',cols,temporary=False,bind=ses.get_bind())
        # import pdb;pdb.set_trace()
        #fill the table with the file list and last modification timsstamps
        count=0
        for uri in urilist:
            entry=tmptable(uri=uri.url,lastmod=uri.lastmod)
            ses.add(entry)
            count+=1
            if count > self.commitperN:
                ses.commit()
                count=0

        ses.commit()

        #delete all entries which require updating
        # first gather all the ids of i entries which are expired 
        subqry=ses.query(self.table.id).join(tmptable, and_(tmptable.uri == self.table.uri,tmptable.lastmod > self.table.lastupdate)).subquery()
        # #then delete those entries from the table
        # import pdb;pdb.set_trace()
        
        delqry=self.table.__table__.delete().where(self.table.id.in_(subqry))
        ses.execute(delqry)


        #now make a list of new uris
        qrynew=ses.query(tmptable).outerjoin(self.table,self.table.uri == tmptable.uri).filter(self.table.uri == None)

        #submit transaction
        trans.commit()
        #return entried which need updating he entries in the original table which need updating
        return [UriFile(x.uri,x.lastmod) for x in qrynew]


    def entryNeedsUpdate(self,likestr,lastmod,col=None):
        """Query for a Columns in the table based on a alike string and delete the entry when older than lastmod"""
        needsupdate=True
        try:
            if not col:
                col=self.table.uri
            qResults=self._ses.query(self.table).filter(col.like('%'+likestr+'%'))
            if qResults.count() == 0:
                return True
            needsupdate=False
            #check if at least one needs updating
            for qres in qResults:
                if qres.lastupdate < lastmod:
                    needsupdate=True
                    break

            if needsupdate:
                for qres in qResults:
                    #delete the entries which need updating
                    self._ses.delete(qres)
                    self._ses.commit()
            else:
                slurplogger().info("No Update needed, skipping %s"%(likestr))

        except Exception as e:
            # Fine no entries found
            pass
        return needsupdate

    def addEntry(self,metadict):
        if self.stripuri and "uri" in metadict:
            metadict["uri"]=self.conf.mirrorMap.strip(metadict["uri"])

        entry=self.table(**metadict)
        

        self._ses.add(entry)

        if self.commitCounter > self.commitperN:
            # commit every so many rows
            self._ses.commit()
            self.commitCounter=0
        else:
            self.commitCounter+=1

    def bulkInsert(self,dictlist):
        """Insert a  list of dicts in bulk mode"""
        self._ses.bulk_insert_mappings(self.table,dictlist)


    def truncateTable(self):
        """Truncate all entries in a table"""
        self.db.truncateTable(self.name,self.scheme.lower())

    def createTable(self,cols=None,session=None):
        """dynamically creates a table (when it does not exists) from a list of colums"""
        if self.table == None:
            if cols == None:
                raise RuntimeError("Creating a dynamic table requires the specification of columns")
            self.table=Table(self.name, self.db.mdata, *cols, schema=self.scheme)
            self.table.create(checkfirst=True)
            tableMap=tableMapFactory(self.name,self.table)
            self.table=tableMap
        else:
            if cols != None:
                raise RuntimeError("Cannot create static table from dynamic columns ")
            self.table.__table__.create(self.db.dbeng,checkfirst=True)

        if session:
            # bind the table to a specific session
            session.bind_table(self.table,session.get_bind())
            session.commit()

    def dropTable(self):
        self.db.dropTable(self.name,self.scheme.lower())

    def migrate(self,version):
        """Properly migrate a table between software versions
        (note this function is supposed to be overridden in a derived class)"""
        if version < self.version:
            raise RuntimeError("No migration implemented")
        if version > self.version:
            raise RuntimeError("Registered database has a higher version number than supported")

