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

from .dataProviders.ftpProvider import ftpProvider as ftp
import os,re,sys
import datetime
import zipfile

class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    
    ###### COMPULSARY FUNCTIONS #######
    def __init__(self):
        """Setup main urls, and retrieve already registered plugins from the database"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.name=type(self).__name__
        self.initdb=False
        #plugin version (needs to be updated for breaking changes)
        pluginVersion=(0,0,0)
        #retrieve inventory from the database (or create a default one)
        self.dbinvent=self.conf.db.getDataSource(self.name)
        #get registered plugin version
        self.dbinvent['version']=tuple(self.dbinvent['version'])
        
        #possibly resolve plugin version differences here
        self.dbinvent['version']=pluginVersion
        self.dbinvent['datadir']=self.conf.getDataDir(self.name)
        self.dbinvent['structure']={"data":"jsonb"}
        
        
    
    def executeaction(self,args):
        """Download/update data and apply possible processing"""
        self.download(args.force)


    def addParserArgs(self,subparsers):
        """adds GSHHG specific help options"""
        parser = subparsers.add_parser(self.name, help=type(self).__doc__)

    def initDB(self,db):
        """function which registers the datasource, initializes the associated dataset table and possibly register stored POSTGRESQL procedures"""
        self.conf.db.setDataSource(self.dbinvent)
        DBdataset=DataSetEntry(self.name,self.dbinvent['structure'])
        DBdataset.initTable(self.conf.db)
    ###### END COMPULSARY FUNCTIONS #######
    
    def download(self,force):
        ftplist=self.ftpt.getftplist('gshhg-shp.*zip')
        #first find out the newest version
        vregex=re.compile('gshhg-shp-([0-9]\.[0-9]\.[0-9]).*zip')
        newestver=(0,0,0)
        getf=''
        
        #find out the newest version
        for t,fname in ftplist:
            match=vregex.findall(fname)
            ver=tuple(int(x) for x in match[0].split('.'))
            if ver > newestver:
                newestver=ver
                getf=fname

        #now determine whether to retrieve the file
        if force or (newestver > self.dbinvent["data"]["GSHHGversion"] and t > self.dbinvent["lastupdate"]):
            fout=os.path.join(self.conf.getCacheDir(self.name),getf)
            if os.path.exists(fout) and not force:
                print ("File already in cache no need to download")
            else:
                with open(fout,'wb') as fid:
                    print("Downloading "+getf,file=sys.stderr)
                    self.ftpt.downloadFile(getf,fid)

            self.dbinvent["data"]["GSHHGversion"]=newestver
            self.dbinvent["lastupdate"]=datetime.datetime.now()
            self.unzip(fout)
            #update database inventory
            self.conf.db.setDataSource(self.dbinvent)
            makeDataDescriptors()
        else:
            print("Already at newest version",file=sys.stderr)
            return
        
    def unzip(self,zipf):
        print("Unzipping shapefiles")
        with zipfile.ZipFile(zipf,'r') as zp:
            self.dbinvent["misc"]["filelist"]=zp.namelist()
            zp.extractall(self.datadir)
    
    def makeDataDescriptors(self):
        """ create a list of data descriptors and submit them to the database"""
        # dummy=self.conf.db.getDataDescriptor(self.name,
        
PlugName=GSHHG
