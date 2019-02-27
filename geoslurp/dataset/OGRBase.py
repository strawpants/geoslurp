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
from osgeo import gdal
from geoslurp.config.slurplogger import slurplogger
from geoalchemy2 import WKBElement,Geography
from sqlalchemy import Table,Column, Integer, String, Float
from geoslurp.db import tableMapFactory
import re

def columnsFromOgrFeat(feat, spatindex=True, forceGType=None):
    """Returns a list of columns from a osgeo feature"""
    gisMap = {'String': String, 'Integer': Integer, 'Real': Float, 'Float': Float}
    df = feat.GetDefnRef()
    cols = [Column('id', Integer, primary_key=True)]
    for i in range(feat.GetFieldCount()):
        fld = df.GetFieldDefn(i)
        name = fld.GetName()
        if name.lower() == 'id':
            # skip columns with id (will be  renewed)
            continue
        cols.append(Column(name.lower(), gisMap[fld.GetTypeName()]))

    # append geometry column
    if forceGType:
        gType = forceGType
    else:
        gType = feat.geometry().GetGeometryName()
    geomtype = Geography(gType, srid='4326', spatial_index=spatindex)
    cols.append(Column('geom', geomtype))
    return cols

def valuesFromOgrFeat(feat, encoding='iso-8859-1'):
    """Returns a dictionary with loaded values from a feature"""
    df=feat.GetDefnRef()

    vals={}
    for i in range(feat.GetFieldCount()):
        fld=df.GetFieldDefn(i)
        name=fld.GetName()
        if name.lower() == 'id':
            #skip columns with id (will be automatically filled)
            continue
        if fld.GetTypeName() =='String':
            # vals[name.lower()]=feat.GetFieldAsBinary(i).decode('iso-8859-1')
            vals[name.lower()]=feat.GetFieldAsBinary(i).decode(encoding)
        else:
            vals[name.lower()]=feat.GetField(i)

    #append geometry values
    vals['geom']=WKBElement(feat.geometry().ExportToWkb(),srid=4326)
    return vals

class OGRBase(DataSet):
    """Base class which downloads a single OGR layer (e.g. shapefile) and registers it as a postgis table"""
    table=None
    gtype=None
    ogrfile=None
    encoding='iso-8859-1' #default for shapefiles
    layerregex=None
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
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
        
        #open shapefile directory

        shpf=gdal.OpenEx(self.ogrfile,0)
        
        count=0
        for ithlayer in range(shpf.GetLayerCount()):
            shpflayer=shpf.GetLayer(ithlayer)
            if self.layerregex:
                if not re.search(self.layerregex,shpflayer.GetName()):
                    continue
            for feat in shpflayer:
                count+=1
                #we need to make a temporary clone here as osgeo will cause a segfault otherwise
                if self.table == None:
                    cols=columnsFromOgrFeat(feat,forceGType=self.gtype)
                    self.table=Table(self.name, self.db.mdata, *cols, schema=self.scheme)
                    self.table.create(checkfirst=True)
                    tableMap=tableMapFactory(self.name,self.table)
                    self.table=tableMap
                values=valuesFromOgrFeat(feat,self.encoding)
                try:
                    self.addEntry(values)
                except:
                    pass
                #commit every X times
                

        #also update entry in the inventory table
        self.updateInvent()

