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

from .URLtool import URLtool
from .DBclient import DBclient
import os,re,sys
import datetime
from pymongo import MongoClient
class GSHHG():
    """ Get and update the Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    def __init__(self,conf):
        """Setup main urls"""
        self.url='ftp://ftp.soest.hawaii.edu/gshhg/'
        self.conf=conf #stores a reference to  the global configuration file
        self.invent={"version":(0,0,0),"lastupdate":datetime.datetime(datetime.MINYEAR,1,1)}

        self.datadir=os.path.join(conf['DataDir'],type(self).__name__)
        self.cachedir=os.path.join(conf['CacheDir'],type(self).__name__)
        if not os.path.exists(self.datadir):
            os.makedirs(self.datadir)
        
        if not os.path.exists(self.cachedir):
            os.makedirs(self.cachedir)

        #Setup a mongodb connector
        self.dbclient=MongoClient(self.conf['Mongo'])
        dbinventory=self.dbclient.geoslurp.inventory

        #retrieve info on last update
        dbentry=dbinventory.find({"Name":type(self).__name__})    
        if dbentry.count() == 1:
            self.invent=dbentry[0]     
        
        
    
    def update(self):
        self.urlt=URLtool(self.url)
        ftplist=self.urlt.getftplist('gshhg-shp.*zip')
        #first find out the newest version
        vregex=re.compile('gshhg-shp-([0-9]\.[0-9]\.[0-9]).*zip')
        newestver=(0,0,0)
        getf=''

        for t,fname in ftplist:
            match=vregex.findall(fname)
            ver=tuple(int(x) for x in match[0].split('.'))
            if ver > newestver:
                newestver=ver
                getf=fname
        #now determine whether to retrieve the file
        if newestver > self.invent["version"] and t > self.invent["lastupdate"]:
            fid=open(os.path.join(self.cachedir,getf),'wb')
            print("Downloading "+getf,file=sys.stderr)
            self.urlt.downloadFile(getf,fid)
            self.invent["version"]=newestver
            self.invent["lastupdate"]=datetime.datetime.now()
            fid.close()
        else:
            print("Already at newest version",file=sys.stderr)
        
        #Add/update the database
        plugincol=self.dbclient.geoslurp.inventory.insert(self.invent)
        
PlugName=GSHHG

