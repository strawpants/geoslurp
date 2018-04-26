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
import zipfile
class GSHHG():
    """ Get and update the Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    def __init__(self,conf):
        """Setup main urls"""
        self.url='ftp://ftp.soest.hawaii.edu/gshhg/'
        self.conf=conf #stores a reference to  the global configuration file
        self.invent={"Name":type(self).__name__,"version":(0,0,0),"lastupdate":datetime.datetime(datetime.MINYEAR,1,1),
                    "MainTable":None,"filelist":[]}

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
            #convert version number from list to tuple
            self.invent['version']=tuple(self.invent['version'])
        
    
    def update(self,force):
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
        if force or (newestver > self.invent["version"] and t > self.invent["lastupdate"]):
            fout=os.path.join(self.cachedir,getf)
            if os.path.exists(fout) and not force:
                print ("File already in cache no need to download")
            else:
                with open(fout,'wb') as fid:
                    print("Downloading "+getf,file=sys.stderr)
                    self.urlt.downloadFile(getf,fid)

            self.invent["version"]=newestver
            self.invent["lastupdate"]=datetime.datetime.now()
            self.postProcess(fout)


        else:
            print("Already at newest version",file=sys.stderr)
            return
        
        #Add/update the database
        print("Updating database")
        self.dbclient.geoslurp.inventory.insert(self.invent)
   
    def postProcess(self,zipf):
        print("Unzipping shapefiles")
        with zipfile.ZipFile(zipf,'r') as zp:
            self.invent["filelist"]=zp.namelist()
            zp.extractall(self.datadir)


    def remove(self):
        """removes the plugin files and database entries"""
        for f in self.invent["filelist"]:
            os.remove(f)

        self.dbclient.geoslurp.inventory.delete_one({"Name":type(self).__name__})


PlugName=GSHHG

