# This file is part of geoslurp
# geoslurp-tools is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# geoslurp-tools is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2020

import os
import geopandas as gpd
import pandas as pd
from geoslurp.tools.shapelytools import shpextract
from geoslurp.db.settings import MirrorMap
import re
import tarfile
from sqlalchemy import create_engine
import json
import gzip
import shutil

class MirrorMap:
    def __init__(self,from_mirror,to_mirror):
        if from_mirror.endswith("/"):
            self.from_mirror=from_mirror
        else:
            self.from_mirror=from_mirror+"/"
        
        if to_mirror.endswith("/"):
            self.to_mirror=to_mirror
        else:
            self.to_mirror=to_mirror+"/"
            
    def apply(self,url):
        return url.replace(self.from_mirror,self.to_mirror)
    
    def reverseApply(self,url):
        return url.replace(self.to_mirror,self.from_mirror)
    
    def strip(self,url):
        return url.replace(self.from_mirror,"")


def exportGeoQuery(qryresult,outputfile,layer=None,driver="GPKG",packFiles=False,striproot=None):
    #just add a check and pass to exportQuery function

    if not "geom" in qryresult.keys():
        raise RunTimeError("no geometry found in the specified query")
    exportQuery(qryresult,outputfile,layer=layer,driver=driver,packFiles=packFiles,striproot=striproot)


def exportQuery(qryresult,outputfile,layer=None,driver="SQLITE",packFiles=False,striproot=None):
    """Export a query without a geometry column, and possibly pack corresponding files"""
    
    if "geom" in qryresult.keys():
        useGeoPandas=True
    else:
        useGeoPandas=False 
        
    if useGeoPandas and driver not in ["GPKG"]:    
        raise RunTimeError("no geometry found in the specified query")

    if useGeoPandas:
        df = gpd.GeoDataFrame()
        df["geometry"]=None
    else:
        df=pd.DataFrame()

    #initialize columns
    for ky in (ky for ky in qryresult.keys() if ky not in ["geom","id"]):
        df[ky]=None
   
    if packFiles:
        #open a tgz archive
        farchive=os.path.splitext(outputfile)[0]+"_files.tgz"
        farchivetmp=os.path.splitext(outputfile)[0]+"_files.tar"
        if striproot:
            mmap=MirrorMap(striproot,farchive+":/")

        #open/reopen archive
        if os.path.exists(farchive):
            #unzip to a temporary file
            farchivetmp=os.path.splitext(outputfile)[0]+"_files.tar"
            with gzip.open(farchive,'rb') as gzid:
                with open(farchivetmp,'wb') as fid:
                   shutil.copyfileobj(gzid,fid)

            tarar=tarfile.open(farchivetmp,mode='a')
        else:
            tarar=tarfile.open(farchive,mode='w:gz')

    else:
        tarar=None

    #add new rows
    for entry in qryresult:
        entrymod={}
        for ky,val in entry.items():
            if useGeoPandas and ky == "geom":
                val=shpextract(entry)
                ky="geometry"
            elif ky == "id":
                val=None
            elif ky == "data":
                #convert to json
                val=json.dumps(val)
            elif ky == "uri" and packFiles:
                #modify the uri and add file in the archive
                if not striproot:
                    #try stripping of everything before and including 'geoslurp' from the path
                    striproot=re.search("^.*/geoslurp/",val).group(0)
                    mmap=MirrorMap(striproot,farchive+":/")
                uriorig=val
                val=mmap.apply(val)
                #create a new tarfile member if needed
                try:
                    basef=mmap.strip(uriorig)
                    tinfo=tarar.getmember(basef)
                except KeyError:
                    #create a new member
                    tarar.add(name=uriorig,arcname=basef)
            if val:
                entrymod[ky]=val

        df=df.append(entrymod,ignore_index=True)
    
    if packFiles:
        tarar.close()
        if os.path.exists(farchivetmp):
            #rezip the tar archive and clean up
            with gzip.open(farchive,'wb') as gzid:
                with open(farchivetmp,'rb') as fid:
                   shutil.copyfileobj(fid,gzid)
            os.remove(farchivetmp)
    #export to file
    if useGeoPandas:
        df.to_file(outputfile, layer=layer, driver=driver)
    else:
        if driver == "SQLITE":
            writeToSQLite(outputfile,df,layer)
        else:
            raise InputError("Don't know how to interpret output driver"%driver)

def writeToSQLite(outputfile,df,layer):
    """Write a pandas dataframe as a table to a sqlite file"""
    outeng = create_engine('sqlite:///'+outputfile)
    df.to_sql(layer, outeng, if_exists='replace')



def exportGeoTable(table,outfile,addFiles=False):
    """Export a entire table with a geometry column""" 
    raise NotImplementedError("This functionality is currently not implemented")


def exportTable(table,outfile,addFiles=False):
    """Export a entire table without a geometry column""" 
    raise NotImplementedError("This functionality is currently not implemented")

