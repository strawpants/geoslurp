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

from dataProviders.ftpProvider import ftpProvider as ftp
import os,re,sys
import datetime
import zipfile

class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    def __init__(self,conf):
        """Setup main urls"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.conf=conf #stores a reference to  the global configuration filea
        self.name=type(self).__name__
        #retrieve inventory from the database
        self.dbinvent=self.conf.db.getDataSource(self.name)

        self.datadir=self.conf.getDataDir(self.name)
        self.cachedir=self.conf.getCacheDir(self.name)

        #retrieve info on last update
        # dbentry=dbinventory.find({"Name":type(self).__name__})    
        # if dbentry.count() == 1:
            # self.dbinvent=dbentry[0]
            # #convert version number from list to tuple
            # self.dbinvent['version']=tuple(self.dbinvent['version'])
    def getInvent(self):
        """returns the dictionary which contains the inventory entry"""
        return self.dbinvent
    
    def getDataDescriptors(self):
        """Returns the updated datadescriptor entries"""
        return self.ddescr
    
    def update(self,force):
        ftplist=self.ftpt.getftplist('gshhg-shp.*zip')
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
        if force or (newestver > self.dbinvent["misc"]["version"] and t > self.dbinvent["lastupdate"]):
            fout=os.path.join(self.cachedir,getf)
            if os.path.exists(fout) and not force:
                print ("File already in cache no need to download")
            else:
                with open(fout,'wb') as fid:
                    print("Downloading "+getf,file=sys.stderr)
                    self.urlt.downloadFile(getf,fid)

            self.dbinvent["misc"]["version"]=newestver
            self.dbinvent["lastupdate"]=datetime.datetime.now()
            self.postProcess(fout)


        else:
            print("Already at newest version",file=sys.stderr)
            return
        
    def postProcess(self,zipf):
        print("Unzipping shapefiles")
        with zipfile.ZipFile(zipf,'r') as zp:
            self.dbinvent["misc"]["filelist"]=zp.namelist()
            zp.extractall(self.datadir)


    def remove(self):
        """removes the plugin files and database entries"""
        for f in self.dbinvent["misc"]["filelist"]:
            os.remove(f)

        #self.dbclient.geoslurp.inventory.delete_one({"Name":type(self).__name__})

    def addParserArgs(self,subparsers):
        """adds GSHHG specific help options"""
        parser = subparsers.add_parser(type(self).__name__, help=type(self).__doc__)
PlugName=GSHHG

