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

#some datapull to work with ogr vector files
# NOTE This file will be made obsolete , better use the OGRBaseDset
# RGIDsets and GSHHBase needs updating

from geoalchemy2.elements import WKBElement
from osgeo import ogr
from geoalchemy2 import Geography
from geoslurp.config.slurplogger import slurplogger
from geoslurp.db import tableMapFactory
from sqlalchemy import Table,Column, Integer, String, Float
from geoalchemy2.elements import WKBElement
import re




# def columnsFromFeat(feat, spatindex=True, forceGType=None):
#     """Returns a list of columns from a osgeo feature"""
#     gisMap = {'String': String, 'Integer': Integer, 'Real': Float, 'Float': Float}
#     df = feat.GetDefnRef()
#     cols = [Column('id', Integer, primary_key=True)]
#     for i in range(feat.GetFieldCount()):
#         fld = df.GetFieldDefn(i)
#         name = fld.GetName()
#         if name.lower() == 'id':
#             # skip columns with id (will be  renewed)
#             continue
#         cols.append(Column(name.lower(), gisMap[fld.GetTypeName()]))
#
#     # append geometry column
#     if forceGType:
#         gType = forceGType
#     else:
#         gType = feat.geometry().GetGeometryName()
#     geomtype = Geography(gType, srid='4326', spatial_index=spatindex)
#     # geomtype = Geometry(gType, srid='4326', spatial_index=spatindex)
#     cols.append(Column('geom', geomtype))
#     return cols
#
# def valuesFromFeat(feat):
#     """Returns a dictionary with loaded values from a feature"""
#     df=feat.GetDefnRef()
#
#     vals={}
#     for i in range(feat.GetFieldCount()):
#         fld=df.GetFieldDefn(i)
#         name=fld.GetName()
#         if name.lower() == 'id':
#             #skip columns with id (will be automatically filled)
#             continue
#         if fld.GetTypeName() =='String':
#             vals[name.lower()]=feat.GetFieldAsBinary(i).decode('iso-8859-1')
#         else:
#             vals[name.lower()]=feat.GetField(i)
#
#     #append geometry values
#     vals['geom']=WKBElement(feat.geometry().ExportToWkb(),srid=4326)
#     return vals
#
# def fillGeoTable(folder, tablename, scheme, regex=None, forceGType=None):
#     """Update/populate a database table (creates one if it doesn't exist)
#     This function reads all layers in the shapefile directory whose name obeys
#     the regex and puts them in a single table.
#     :param folder: Folder containing shapefiles
#     :param tablename: name of the resulting table
#     :param scheme: An instance of a derived class fom schemeBase
#     :param regex (string,optional): a layer regex to allow selecting a subseet of layers (defauult takes all layers)
#     :param forceGType (optional): a geometry type to be used as the "geom" column
#     :returns nothing
#     """
#     table=None
#     ses=scheme.db.Session()
#     # currently we can only cope with updating the entire table as a whole
#     scheme.dropTable(tablename)
#     # if self.dbeng.has_table(tablename,schema=schema):
#     slurplogger().info("Filling POSTGIS table %s.%s with data from %s" % (scheme._schema, tablename, folder))
#     #open shapefile directory
#     shpf=ogr.Open(folder)
#     for il in range(shpf.GetLayerCount()):
#         #check for regex
#         if regex:
#             if not bool(re.search(regex,shpf[il].GetName())):
#                 continue
#         for ift in range(shpf[il].GetFeatureCount()):
#             #we need to make a emporary clone here as osgeo will cause a segfault otherwise
#             feat=shpf[il][ift].Clone()
#
#             if table == None:
#                 cols=columnsFromFeat(feat,forceGType=forceGType)
#                 table=Table(tablename, scheme.db.mdata, *cols, schema=scheme._schema)
#                 table.create(checkfirst=True)
#                 tableMap=tableMapFactory(tablename,table)
#             values=valuesFromFeat(feat)
#             try:
#                 ses.add(tableMap(**values))
#             except:
#                 pass
#     ses.commit()
#     ses.close()

