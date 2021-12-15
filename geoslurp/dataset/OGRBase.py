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

from geoslurp.dataset.dataSetBase import DataSet
from osgeo import gdal,osr
from geoslurp.config.slurplogger import slurplogger
from geoalchemy2 import WKBElement,Geography,Geometry
from sqlalchemy import Column, Integer, String, Float, BigInteger,Date,DateTime
from geoslurp.db import tableMapFactory
import re
from zipfile import ZipFile
import os



class OGRBase(DataSet):
    """Base class which downloads a single OGR layer (e.g. shapefile) and registers it as a postgis table"""
    table=None
    gtype=None
    ogrfile=None
    encoding='iso-8859-1' #default for shapefiles
    layerregex=None
    targetprj=None
    targetsrid=4326
    swapxy=False
    ignoreFields=None
    spatindex=True
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #set default target projection if not explicitly set
        if not self.targetprj:
            self.targetprj = osr.SpatialReference()
            self.targetprj.ImportFromEPSG(self.targetsrid)

        if self.ignoreFields:
            self.ignoreRegex=re.compile(self.ignoreFields)
        else:
            #make a regex which will never match
            self.ignoreRegex=re.compile('(?!x)x')
        

    def valuesFromOgrFeat(self,feat,transform=None):
        """Returns a dictionary with loaded values from a feature"""
        df=feat.GetDefnRef()
        
                
            
        vals={}
        for i in range(feat.GetFieldCount()):
            fld=df.GetFieldDefn(i)
            name=fld.GetName()
            
            if self.ignoreRegex.search(name):
                continue
            
            if name.lower() == 'id':
                #skip columns with id (will be automatically filled)
                continue
            if fld.GetTypeName() =='String':
                # vals[name.lower()]=feat.GetFieldAsBinary(i).decode('iso-8859-1')
                vals[name.lower()]=feat.GetFieldAsBinary(i).decode(self.encoding)
            else:
                vals[name.lower()]=feat.GetField(i)

        #append geometry values

        geom=feat.GetGeometryRef()

        if self.swapxy: 
            geom.SwapXY()

        if transform:
            geom.Transform(transform)

        vals['geom']=WKBElement(geom.ExportToWkb(),srid=self.targetsrid)

        return vals

    def columnsFromOgrFeat(self,feat):
        """Returns a list of columns from a osgeo feature"""
        gisMap = {'String': String, 'Integer': Integer, 'Real': Float, 'Float': Float, 'Integer64':BigInteger, "Date":Date,"DateTime":DateTime}
        df = feat.GetDefnRef()
        cols = [Column('id', Integer, primary_key=True)]
        for i in range(feat.GetFieldCount()):
            fld = df.GetFieldDefn(i)
            name = fld.GetName()
            if self.ignoreRegex.search(name):
                continue
            
            if name.lower() == 'id':
                # skip columns with id (will be  renewed)
                continue
                
            cols.append(Column(name.lower(), gisMap[fld.GetTypeName()]))

        if feat.geometry().Is3D():
            geomdim=3
        else:
            geomdim=2

        # append geometry column
        if self.gtype:
            gType = self.gtype

        else:
            gType = feat.geometry().GetGeometryName()

        if self.targetsrid == 4326:
            geomtype = Geography(gType, srid=self.targetsrid, spatial_index=self.spatindex,dimension=geomdim)
        else:
            geomtype = Geometry(gType, srid=self.targetsrid, spatial_index=self.spatindex,dimension=geomdim)

        cols.append(Column('geom', geomtype))
        return cols
    
    
    def register(self):
        """Update/populate a database table (creates one if it doesn't exist)
        This function reads a shapefile and puts it in a single table.
        :param ogrfile: gdal dataset (e.g. shapefile)
        :param forceGType (optional): a geometry type to be used as the "geom" column
        :returns nothing (but sets the internal qlalchemy table)
        """
        # currently we can only cope with updating the entire table as a whole
        self.db.dropTable(self.name,self.scheme)

        slurplogger().info("Filling POSTGIS table %s.%s with data from %s" % (self.scheme, self.name, self.ogrfile))
        
        #open shapefile directory or ogr file
        if self.ogrfile.endswith('.kmz') and not gdal.GetDriverByName('LIBKML'): 
            #unzip the kmz file
            cache=self.cacheDir()
            with ZipFile(self.ogrfile,'r') as zp:
                kmlf=zp.namelist()[0]#take the first zip file only 
                zp.extract(kmlf,cache)
            kmlfile=os.path.join(cache,kmlf)
            shpf=gdal.OpenEx(kmlfile,0)

        else:
            shpf=gdal.OpenEx(self.ogrfile,0)
        
        count=0
        for ithlayer in range(shpf.GetLayerCount()):
            shpflayer=shpf.GetLayer(ithlayer)
            if self.layerregex:
                if not re.search(self.layerregex,shpflayer.GetName()):
                    continue
            sourceprj = shpflayer.GetSpatialRef()
            if sourceprj.IsSame(self.targetprj):
                transform=None
            else:
                transform = osr.CoordinateTransformation(sourceprj, self.targetprj)
            # print(sourceprj)

            # print(self.targetprj)
            
            # print(sourceprj.IsSame(self.targetprj))
            for feat in shpflayer:
                count+=1
                if self.table == None:
                    cols=self.columnsFromOgrFeat(feat)
                    self.createTable(cols)
                values=self.valuesFromOgrFeat(feat,transform)
                # import pdb;pdb.set_trace()
                try:
                    self.addEntry(values)
                except:
                    pass
                #commit every X times
                

        #also update entry in the inventory table
        self.updateInvent()

