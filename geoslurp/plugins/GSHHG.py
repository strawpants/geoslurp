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
# plugin for the Global self consistent Hierachical High resolution geography database
from geoslurp.dataProviders.ftpProvider import ftpProvider as ftp
from geoslurp.commonOptions import commonOptions
from geoslurp.slurpconf import Log
import os,re,sys
import datetime
import zipfile
from geoslurp.geoslurpClient import Invent
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm.exc import NoResultFound
from geoalchemy2.elements import WKBElement
from geoalchemy2 import Geometry
from osgeo import ogr
from glob import glob
from sqlalchemy.ext.declarative import declarative_base,declared_attr


#define the plugin name
PlugName='GSHHG'


class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    
    #plugin version (needs to be updated for breaking changes)
    pluginVersion=(0,0,0)
    ###### COMPULSARY FUNCTIONS #######
    def __init__(self,db,conf):
        """Setup main urls, and retrieve already registered plugins from the database"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.name=type(self).__name__
        self.schema=self.name.lower()
        self.datadir=conf['DataDir']
        self.cachedir=conf['CacheDir']
        #Initialize databases (if not existent)
        self.db=db
        try:
            #retrieve the stored inventory entry
            self.dbinvent=self.db.getFromInventory(self.name)
            self.dbinvent.data["GSHHGversion"]=tuple(self.dbinvent.data["GSHHGversion"]) 
        except NoResultFound:
        # #set defaults for the  inventory
            self.dbinvent=Invent(datasource=self.name,pluginversion=self.pluginVersion,lastupdate=datetime.datetime.min,data={"GSHHGversion":(0,0,0)})
            
            # create the schema corresponding tables
            db.CreateSchema(self.schema)
    
    def parseAndExec(self,args):
        """Download/update data and apply possible processing"""

        if args.update or args.download:
            self.download(args.force)
        if args.update or args.register:
            self.register()

    @staticmethod
    def addParserArgs(subparsers):
        """adds GSHHG specific help options (note this is a static function)"""
        parser = subparsers.add_parser(GSHHG.__name__, help=GSHHG.__doc__)
        commonOptions['force'](parser)
        commonOptions['update'](parser)
        commonOptions['register'](parser)
        commonOptions['download'](parser)

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
        if force or (newestver > self.dbinvent.data["GSHHGversion"] and t > self.dbinvent.lastupdate):
            fout=os.path.join(self.cachedir,getf)
            if os.path.exists(fout) and not force:
                print (self.name+":File already in cache no need to download",file=Log)
            else:
                with open(fout,'wb') as fid:
                    print(self.name+":Downloading "+getf,file=Log)
                    self.ftpt.downloadFile(fid,getf)
            
            with ZipFile(fout,'r') as zp:
                zp.extractall(self.cachedir)
            self.dbinvent.data["GSHHGversion"]=newestver
            
        else:
            print(self.name+": Already at newest version",file=Log)
            return

    def register(self):
        for res in ['c','f','h','i','l']:
            self.db.fillGeoTable(os.path.join(self.cachedir,'GSHHS_shp/'+res),tablename='GSHHS_'+res,schema=self.schema)
        
        for res in ['c','f','h','i','l']:
            self.db.fillGeoTable(os.path.join(self.cachedir,'WDBII_shp/'+res),tablename='WDBII_rivers_'+res,schema=self.schema,regex='river')
            self.db.fillGeoTable(os.path.join(self.cachedir,'WDBII_shp/'+res),tablename='WDBII_border_'+res,schema=self.schema,regex='border')
        
        #update database inventory
        self.dbinvent.lastupdate=datetime.datetime.now()
        self.db.updateInventory(self.dbinvent)
    
