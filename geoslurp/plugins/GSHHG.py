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
from geoslurp.commonOptions import commonOptions
import os,re,sys
import datetime
import zipfile
from geoslurp.geoslurpClient import GSBase,Invent
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
from sqlalchemy.orm.exc import NoResultFound
from geoalchemy2 import Geometry,shape
from osgeo import ogr
from glob import glob
from shapely.geometry import shape

class GSHHGTable(GSBase):
    """Defines a dataset table for the GSHHG"""
    __tablename__='GSHHG'
    id=Column(Integer,primary_key=True)
    dataset=Column(String)
    resolution=Column(String)
    layer=Column(String)
    data=Column(Geometry('MULTIPOLYGON'))

class GSHHG():
    """The Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    
    #plugin version (needs to be updated for breaking changes)
    pluginVersion=(0,0,0)
    ###### COMPULSARY FUNCTIONS #######
    def __init__(self,db,conf):
        """Setup main urls, and retrieve already registered plugins from the database"""
        self.ftpt=ftp('ftp://ftp.soest.hawaii.edu/gshhg/')
        self.name=type(self).__name__
        self.datadir=conf.getDataDir(self.name)
        self.cachedir=conf.getCacheDir(self.name)
        #Initialize databases (if not existent)
        self.ses=db.Session()
        try:
            #retrieve the stored inventory entry
            self.dbinvent=self.ses.query(Invent).filter(Invent.datasource == self.name).one()
            self.dbinvent.data["GSHHGversion"]=tuple(self.dbinvent.data["GSHHGversion"]) 
        except NoResultFound:
        # #set defaults for the  inventory
            self.dbinvent=Invent(datasource=self.name,version=self.pluginVersion,lastupdate=datetime.datetime.min,data={"GSHHGversion":(0,0,0)})
            # create the  GSHHG table
            GSBase.metadata.create_all(db.dbeng) 
            self.ses.add(self.dbinvent)
        
    
    def parseAndExec(self,args):
        """Download/update data and apply possible processing"""
        
        if args.update:
            self.download(args.force)

    @staticmethod
    def addParserArgs(subparsers):
        """adds GSHHG specific help options (note this is a static function)"""
        parser = subparsers.add_parser(GSHHG.__name__, help=GSHHG.__doc__)
        commonOptions['force'](parser)
        commonOptions['update'](parser)

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
                print ("File already in cache no need to download")
            else:
                with open(fout,'wb') as fid:
                    print("Downloading "+getf,file=sys.stderr)
                    self.ftpt.downloadFile(getf,fid)
            self.unzip(fout)
            self.dbinvent.data["GSHHGversion"]=newestver
            self.dbinvent.lastupdate=datetime.datetime.now()
            self.register()
            #update database inventory
            self.ses.commit() 
            self.ses.flush()
        else:
            print("Already at newest version",file=sys.stderr)
            return
        
    def unzip(self,zipf):
        print("Unzipping shapefiles in cache directory")
        with zipfile.ZipFile(zipf,'r') as zp:
            zp.extractall(self.cachedir)
    
    def register(self):
        """Register shapefiles layers in the postgis database"""

        ncount=self.ses.query(GSHHGTable).count()
        
        if (ncount == 0):
            #loop over directories
            Dsets=['GSHHS_shp','WDBII_shp']
            resolution=['c', 'f',  'h',  'i',  'l']
            for Ds in Dsets:
                for res in resolution:
                    #open shapefile directory
                    shpf=ogr.Open(os.path.join(self.cachedir,Ds,res))
                    #loop over layers
                    for ithlayer in range(shpf.GetLayerCount()):
                        #loop over features
                        for ithfeat in range(shpf.GetLayer(ithlayer).GetFeatureCount()):
                            
                            shpobj=shpf[ithlayer][ithfeat].geometry().ExportToWkb()
                            #create a database table entry
                            entry=GSHHGTable(dataset=Ds,resolution=res,layer=shpf.GetLayer(ithlayer).GetName(),data=shpobj)
                            self.ses.add(entry)

PlugName=GSHHG
