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
import logging

class DataSet(ABC):
    """Abstract Base class which hold a dataset (corresponding to a database table"""
    table=None
    ses=None
    commitCounter=0
    def __init__(self,scheme):
        self.name=self.__class__.__name__.lower()
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

    def purge(self):
        """Delete dataset entry"""
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
                logging.info("No Update needed, skipping %s"%(urilikestr))

        except Exception as e:
            # Fine no entries found
            pass
        return needsupdate

    def addEntry(self,metadict):
            entry=self.table(**metadict)
            self.ses.add(entry)

            if self.commitCounter > 10:
                # commit every so many rows
                self.ses.commit()
                self.commitCounter=0
            else:
                self.commitCounter+=1
