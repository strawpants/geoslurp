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
#  Scheme for clustering gravity related data (GRACE, GRACEFO, dealiasing data, static fields)


import os
from geoslurp.schema import Schema
from geoslurp.dataset import GRACEdict,ICGEM_static


class Gravity(Schema):
    """A scheme which contains datasets related to gravity fields"""
    __datasets__={**GRACEdict(), "ICGEM_static":ICGEM_static}
    def __init__(self,InventInstance, conf):
        super().__init__(InventInstance, conf)




# from geoslurp.dataProviders.ftpProvider import ftpProvider as ftp
# from geoslurp.commonOptions import commonOptions
# from geoslurp.slurpconf import Log
# import os,re,sys
# from datetime import datetime
# from geoslurp.geoslurpClient import Invent,columnsFromDict,tableMapFactory
# import gzip
# from glob import glob
# from sqlalchemy.schema import Table
# # from sqlalchemy import Column, Integer, String, Float
# # from sqlalchemy.dialects.postgresql import TIMESTAMP, ARRAY,JSONB
# # from sqlalchemy.orm.exc import NoResultFound
# # from geoalchemy2.elements import WKBElement
# # from geoalchemy2 import Geometry
# # from osgeo import ogr
# # from glob import glob
# # from sqlalchemy.ext.declarative import declarative_base,declared_attr
# # from numpy import arange
# # from shutil import copy
# # import subprocess
#
#
# #define the plugin name
#
# PlugName='GRAV'
#
#
# Dsets={
#     'GFZL2RL05':{'ftp':ftp("ftp://podaac-ftp.jpl.nasa.gov/allData/grace/L2/GFZ/RL05/",maxconnections=8),
#         'description':"Level 2 Stokes coefficients from GFZ release 05a"},
#     'CSRL2RL05':{'ftp':ftp("ftp://podaac-ftp.jpl.nasa.gov/allData/grace/L2/CSR/RL05/",maxconnections=8),
#         'description':"Level 2 Stokes coefficients from CSR release 05"}
#     }
#
#
# def getGSMMeta(shfile):
#     """function which extracts meta information from a GSM file and returns it in a dict"""
#     meta={'data':{}}
#     with gzip.open(shfile,'rb') as gzfid:
#         ln=gzfid.readline()
#         if ln[0:5] != b'FIRST':
#             raise RuntimeError('File seems to be in the wrong format: '+shfile)
#         for ln in gzfid:
#             lnspl=ln.split()
#             # print(lnspl[0])
#             if lnspl[0] == b'EARTH':
#                 meta['data']['gm']=float(lnspl[1])
#                 meta['data']['re']=float(lnspl[2])
#                 continue
#             if lnspl[0] == b'SHM':
#                 meta['data']['nmax']=int(lnspl[1])
#                 mtch=re.search(b'(exclusive|inclusive) permanent tide',ln)
#                 if mtch:
#                     if mtch.group(1) == b"inclusive":
#                         meta['data']['tidesystem']='zero tide'
#                     else:
#                         meta['data']['tidesystem']='tide free'
#                 continue
#             if lnspl[0] ==b'GRCOF2':
#                 #extract start and end time of the data
#                 meta['tstart']=datetime.strptime(lnspl[7].decode('utf-8'),'%Y%m%d.%H%M')
#                 meta['tend']=datetime.strptime(lnspl[8].decode('utf-8'),'%Y%m%d.%H%M')
#                 #only read this once and assume all coefficients will have the same time span
#                 break
#     return meta
#
# def extractMeta(shfile):
#     """Function which extracts meta info from a GRACE file"""
#     meta={'product':None,'tstart':None,'tend':None,'uri':'file://'+shfile,'data':{}}
#     mtchL2SH=re.search('(G[AS][ABCDM])[^/]+(GK2-|G---|GOL-)[^/]+\.(gz)$',shfile)
#     if mtchL2SH:
#         meta.update(getGSMMeta(shfile))
#         meta['product']=mtchL2SH.group(1)
#         if mtchL2SH.group(1) == 'GSM':
#             if mtchL2SH.group(2) == 'GK2-' or mtchL2SH.group(2) == 'GOL-':
#                 meta['data']['constraint']=True
#             else:
#                 meta['data']['constraint']=False
#
#     return meta
#
#
# class GRAV():
#     """Download and manage GRAV(ity) products"""
#
#     #plugin version (needs to be updated for breaking changes)
#     pluginVersion=(0,0,0)
#     ###### COMPULSARY FUNCTIONS #######
#     def __init__(self,db,conf):
#         """Setup main urls, and retrieve already registered plugins from the database"""
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
#         except NoResultFound:
#         # #set defaults for the  inventory
#             self.dbinvent=Invent(datasource=self.name,pluginversion=self.pluginVersion,lastupdate=datetime.min,data={})
#
#             # create the schema
#             db.CreateSchema(self.schema)
#
#
#     def parseAndExec(self,args):
#         """Download/update data and apply possible processing"""
#
#         if args.show:
#             for ky,val in Dsets.items():
#                 print(ky+": "+val['description'])
#                 sys.exit(0)
#
#         if not args.dataset in Dsets:
#             raise ValueError("Dataset is not supported: "+args.dataset)
#         registerFiles=None
#         if args.update or args.download:
#             registerFiles=self.download(args.force,args.dataset)
#             #quit directly if nothing was updated
#             if args.update and not registerFiles:
#                 print("Nothing to update",file=Log)
#
#         if args.update or args.register:
#                 self.register(args.dataset,registerFiles)
#
#
#     @staticmethod
#     def addParserArgs(subparsers):
#         """adds GRAVity specific help options (note this is a static function)"""
#         parser = subparsers.add_parser(__class__.__name__, help=__class__.__doc__)
#         commonOptions['force'](parser)
#         commonOptions['update'](parser)
#         commonOptions['register'](parser)
#         commonOptions['download'](parser)
#         parser.add_argument('--dataset',type=str,default='GFZL2RL05',help='Specify the dataset. Choose one of '+''.join([ds +'\n' for ds in Dsets] ))
#         parser.add_argument('--show',action='store_true',help='Show supported datasets')
#
#     ###### END COMPULSARY FUNCTIONS #######
#     def download(self,force,dataset):
#
#         ftproot=Dsets[dataset]['ftp']
#         #only download files when needed
#         return ftproot.updateFiles(os.path.join(self.datadir,dataset),'G*gz',log=Log)
#
#
#
#
#     def register(self,dataset,updatedFiles=None):
#         """Process and register the dataset
#         If force=True all downloaded data in the dataset is reindexed
#         """
#         tname=dataset.lower()
#
#         if updatedFiles == None:
#             #Reregister all the files again (drops previous table)
#             #make a list of all files in the dataset directory
#             files=glob(os.path.join(self.datadir,dataset,'G*gz'))
#             self.db.dropTable(tname,self.schema)
#         else:
#             files=updatedFiles
#
#
#         tableMap=None
#         ses=self.db.Session()
#         #loop over just downloaded files        for f in  self.updatedfiles:
#         for f in  files:
#             meta=extractMeta(f)
#             if tableMap == None:
#                 cols=columnsFromDict(meta)
#                 table=Table(tname,self.db.mdata,*cols,schema=self.schema)
#                 table.create(checkfirst=True)
#                 tableMap=tableMapFactory(tname,table)
#
#             #first try updating an old record
#             with self.db.dbeng.connect() as conn:
#                 upd=table.update().where(table.c.uri== meta['uri']).values(**meta)
# #                import pdb;pdb.set_trace()
#                 res=conn.execute(upd)
#                 if res.rowcount == 1:
#                     print('Updating old entry '+meta['uri'],file=Log)
#                 else:
#                     #try adding a new object
#                     print("Registering new entry "+meta['uri'],file=Log)
#                     ses.add(tableMap(**meta))
#
#         ses.commit()
#         ses.close()
#         self.dbinvent.data[dataset]={"lastupdate":datetime.now().isoformat(),'Description':Dsets[dataset]['description']}
#         self.dbinvent.lastupdate=datetime.now()
#         self.db.updateInventory(self.dbinvent)
#
#         self.db.vacuumAnalyze(tname,self.schema)


