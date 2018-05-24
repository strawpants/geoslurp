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

from geoslurp.dataProviders.ftpProvider import ftpProvider as ftp
import os,re,sys
import datetime
import zipfile
from geoslurp.geoslurpClient import GSBase,Invent
from sqlalchemy import Column, Integer
from sqlalchemy.orm.exc import NoResultFound

class GSHHGTable(GSBase):
    """Defines a dataset table for the GSHHG"""
    __tablename__='GSSHHG'
    id=Column(Integer,primary_key=True)

class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    
    #plugin version (needs to be updated for breaking changes)
    pluginVersion=(0,0,0)
    ###### COMPULSARY FUNCTIONS #######
    def __init__(self):
        """Setup main urls, and retrieve already registered plugins from the database"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.name=type(self).__name__
        
        
    
    def parseAndExec(self,args):
        """Download/update data and apply possible processing"""
        
        if args.update:
            self.download(args.force)


    def addParserArgs(self,subparsers):
        """adds GSHHG specific help options"""
        parser = subparsers.add_parser(self.name, help=type(self).__doc__)

    def initDB(self,db):
        """function which retrieves the inventory entry of this datasource. If not found it will create a default inventory entry, initializes the possible datasource specific tables and register datasource specific POSTGRESQL procedures"""
        ses=db.Session()
        try:
            #retrieve the stored inventory entry
            self.dbinvent=ses.query(Invent).filter(Invent.datasource == self.name).one()
        except NoResultFound:
        # #set defaultss for the  inventory
            self.dbinvent=Invent(datasource=self.name,version=self.pluginVersion,lastupdate=datetime.datetime.min,data={})
        #retrieve an existing inventory from the database (or create a default one)
#        self.dbinvent=db.getInventoryEntry(self.name)

 #       if self.dbinvent:
            #convert the version array as a tuple
  #          self.dbinvent['version']=tuple(self.dbinvent['version'])
  #      else:
            #initialize inventory with defaults
   #         self.dbinvent={'version':pluginVersion,"datadir":conf.getDataDir(self.name),"data":{}}
            #also create additional plugin specific tables if needed
    #        self.tablestruct=OrderedDict([("id","serial PRIMARY KEY"),("dataset","varchar"),("data","jsonb")])
     #       db.CreateTable(self.name,self.tablestruct)
            #possibly register functions in the database


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
