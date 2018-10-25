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
#  Schema for downloading the Randalph Glacier inventory


import os
from geoslurp.schema import Schema
from geoslurp.config import getCreateDir
from geoslurp.dataset import getRGIdict
class RGI(Schema):
    """A scheme which contains the datasets from the Global Self-consistent, Hierarchical, High-resolution Geography Database"""
    __datasets__=getRGIdict()
    __version__=(0, 0, 0)
    def __init__(self,InventInstance, conf):
        super().__init__(InventInstance, conf)
        self.cache=getCreateDir(os.path.join(conf["CacheDir"],"RGI"))



# from geoslurp.dataProviders.httpProvider import httpProvider as http
# from geoslurp.commonOptions import commonOptions
# from geoslurp.slurpconf import Log
# import os,re,sys
# import datetime
# from zipfile import ZipFile
# from geoslurp.geoslurpClient import Invent
# from sqlalchemy import Column, Integer, String, Float
# from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
# from sqlalchemy.orm.exc import NoResultFound
# from geoalchemy2.elements import WKBElement
# from geoalchemy2 import Geometry
# from osgeo import ogr
# from glob import glob
# from sqlalchemy.ext.declarative import declarative_base,declared_attr
# from numpy import arange
# from shutil import copy
# import subprocess
# #define the plugin name
# PlugName='RGI'
#
#
#
# class RGI():
#     """The Randalph Glacier Inventory (see http://www.glims.org/RGI/rgi60_dl.html)"""
#
#     #plugin version (needs to be updated for breaking changes)
#     pluginVersion=(0,0,0)
#     ###### COMPULSARY FUNCTIONS #######
#     def __init__(self,db,conf):
#         """Setup main urls, and retrieve already registered plugins from the database"""
#         self.http=http('http://www.glims.org/RGI/rgi60_files/')
#         self.name=type(self).__name__
#         self.schema=self.name.lower()
#         self.datadir=conf['DataDir']
#         self.cachedir=conf['CacheDir']
#         #We need to store the database links beacuse we need them in member functions
#         self.db=db
#
#         try:
#             #retrieve the stored inventory entry
#             self.dbinvent=self.db.getFromInventory(self.name)
#             #convert version numer to tuple
#             self.dbinvent.data["RGIversion"]=tuple(self.dbinvent.data["RGIversion"])
#         except NoResultFound:
#         # #set defaults for the  inventory
#             self.dbinvent=Invent(datasource=self.name,pluginversion=self.pluginVersion,lastupdate=datetime.datetime.min,data={"RGIversion":(0,0)})
#
#             # create the schema
#             db.CreateSchema(self.schema)
#
#
#     def parseAndExec(self,args):
#         """Download/update data and apply possible processing"""
#
#         if args.update or args.download:
#             self.download(args.force)
#
#         if args.update or args.register:
#             self.register()
#
#     @staticmethod
#     def addParserArgs(subparsers):
#         """adds RGI specific help options (note this is a static function)"""
#         parser = subparsers.add_parser(RGI.__name__, help=RGI.__doc__)
#         commonOptions['force'](parser)
#         commonOptions['update'](parser)
#         commonOptions['register'](parser)
#         commonOptions['download'](parser)
#
#     ###### END COMPULSARY FUNCTIONS #######
#
#     def download(self,force):
#         getf='00_rgi60.zip'
#
#
#         #Newest version which is supported by this plugin
#         newestver=(5,0)
#         #now determine whether to retrieve the file
#         if force or (newestver > self.dbinvent.data["RGIversion"]):
#             fout=os.path.join(self.cachedir,getf)
#             if os.path.exists(fout) and not force:
#                 print (self.name+":File already in cache no need to download",file=Log)
#             else:
#                 with open(fout,'wb') as fid:
#                     print(self.name+":Downloading "+getf,file=Log)
#                     self.http.downloadFile(fid,getf)
#             zipd=os.path.join(self.cachedir,'zipfiles')
#             with ZipFile(fout,'r') as zp:
#                 zp.extractall(zipd)
#
#             #now recursively zip the other zip files
#             for zf in glob(zipd+'/*zip'):
#                 print("Unzipping %s"%zf,file=Log)
#                 with ZipFile(zf,'r') as zp:
#                     zp.extractall(os.path.join(self.cachedir,'extract'))
#             self.patch()
#
#             self.dbinvent.data["RGIversion"]=newestver
#         else:
#             print(self.name+": Already at newest version",file=Log)
#             return
#
#     def patch(self):
#         """Patches one csv file which contains a . instead of a , at one point"""
#         pf='04_rgi60_ArcticCanadaSouth_hypso.csv.patch'
#         patchf=os.path.dirname(__file__)+'/../patches/'+pf
#         cdir=os.path.join(self.cachedir,'extract')
#         copy(patchf,cdir)
#         #apply patch
#         subprocess.Popen(['patch','-i',pf],cwd=cdir)
#
#
#     def register(self):
#         """Process and register the data"""
#         for shpf in glob(os.path.join(self.cachedir,'extract')+'/*.shp'):
#             tname=os.path.splitext(os.path.basename(shpf))[0]
#             self.db.fillGeoTable(shpf,tablename=tname,schema=self.schema,forceGType='GEOMETRY')
#
#         #also read in the hypsometry tables (csv)
#         lookup={"RGIId":"String","GLIMSId":"String" ,"Area":"Float"}
#         for elev in arange(25,9000,50):
#             lookup[str(elev)]='Float'
#
#         for csvf in glob(os.path.join(self.cachedir,'extract')+'/*hypso.csv'):
#             tname=os.path.splitext(os.path.basename(csvf))[0]
#             with open(csvf,'r') as fid:
#                 self.db.fillCSVTable(fid,tname,lookup,self.schema)
#
#         #Add the csv table with the summaries
#         sumlookup={"Region":"String","O1":"String", "O2":"String","Count":"Integer",        "Area":"Float","HypArea":"Float"}
#         for elev in arange(25,9000,50):
#             sumlookup[str(elev)]='Float'
#
#         with open(os.path.join(self.cachedir,'extract')+'/00_rgi60_summary.csv','r') as fid:
#             next(fid) #skip first header line
#             self.db.fillCSVTable(fid,'00_rgi60_summary',sumlookup,self.schema)
#
#         #Add the csv table with links to the WGMS mass estimates
#         linklookup={"RGIId":"String","GLIMSId":"String","Name":"String","Area":"Float","CenLon":"Float","CenLat":"Float","FoGId":"String","PolUnit":"String"}
#         with open(os.path.join(self.cachedir,'extract')+'/00_rgi60_links.csv','r') as fid:
#             next(fid) #skip first 2 header line
#             next(fid)
#             self.db.fillCSVTable(fid,'00_rgi60_links',linklookup,self.schema)
#
#
#         self.dbinvent.lastupdate=datetime.datetime.now()
#         self.db.updateInventory(self.dbinvent)
#
#
#
#
#

