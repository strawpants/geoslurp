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
    ses=None
    commitCounter=0
    def __init__(self,scheme):
        self.name=self.__class__.__name__.lower().replace('-',"_")
        self.scheme=scheme
        #retrieve the Dataset entry (in the form of a dictionary) from the Inventory
        try:
            self._inventData=scheme._dbinvent.datasets[self.name]
        except KeyError:
            self._inventData={}

    def updateInvent(self):
        self.scheme.updateInvent(self.name,self._inventData)

    def info(self):
        return self._inventData

    def dataDir(self,subdirs=None):
        """Returns the specialized data directory of this scheme and dataset
        The directory will be created if it does not exist"""
        return self.scheme.conf.getDir(self.scheme.__class__.__name__, 'DataDir', dataset=self.__class__.__name__,subdirs=subdirs)

    def cacheDir(self,subdirs=None):
        """returns the cache directory of this scheme and dataset"""
        return self.scheme.conf.getDir(self.scheme.__class__.__name__, 'CacheDir', dataset=self.__class__.__name__,subdirs=subdirs)

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

    def purgeentry(self,filter):
        """Delete dataset entry in the database"""
        self._inventData={None}
        del self.scheme._dbinvent.datasets[self.name]
        self.scheme._ses.commit()
        self.scheme.dropTable(self.name)

    def halt(self):
        pass

    def uriNeedsUpdate(self, urilikestr,lastmod):
        """Query for a URI in the table based on a alike string and delete the entry when older than lastmod"""
        if not self.ses:
            self.ses=self.scheme.db.Session()

        needsupdate=True
        try:
            qResults=self.ses.query(self.table).filter(self.table.uri.like('%'+urilikestr+'%'))
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
                    self.ses.delete(qres)
                    self.ses.commit()
            else:
                slurplogger().info("No Update needed, skipping %s"%(urilikestr))

        except Exception as e:
            # Fine no entries found
            pass
        return needsupdate

    def entryNeedsUpdate(self,likestr,lastmod,col=None):
        """Query for a Columns in the table based on a alike string and delete the entry when older than lastmod"""
        if not self.ses:
            self.ses=self.scheme.db.Session()

        needsupdate=True
        try:
            if not col:
                col=self.table.uri
            qResults=self.ses.query(self.table).filter(col.like('%'+likestr+'%'))
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
                    self.ses.delete(qres)
                    self.ses.commit()
            else:
                slurplogger().info("No Update needed, skipping %s"%(likestr))

        except Exception as e:
            # Fine no entries found
            pass
        return needsupdate

    def addEntry(self,metadict):
        entry=self.table(**metadict)
        if not self.ses:
            self.ses=self.scheme.db.Session()

        self.ses.add(entry)

        if self.commitCounter > 10:
            # commit every so many rows
            self.ses.commit()
            self.commitCounter=0
        else:
            self.commitCounter+=1


    def clearTable(self):
        """Clears all entries in a table"""
        if not self.ses:
            self.ses=self.scheme.db.Session()
        allrows=self.ses.query(self.table)
        for res in allrows:
            self.ses.delete(res)
