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


from geoalchemy2.elements import WKBElement
from osgeo import ogr
from geoalchemy2 import Geometry

def columnsFromFeat(feat, spatindex=True, forceGType=None):
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
    geomtype = Geometry(gType, srid='4326', spatial_index=spatindex)
    cols.append(Column('geom', geomtype))
    return cols

def valuesFromFeat(feat):
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
            vals[name.lower()]=feat.GetFieldAsBinary(i).decode('iso-8859-1')
        else:
            vals[name.lower()]=feat.GetField(i)

    #append geometry values
    vals['geom']=WKBElement(feat.geometry().ExportToWkb(),srid=4326)
    return vals

def fillGeoTable(self,folder,tablename,schema,regex=None,forceGType=None):
    """Update/populate a database table (creates one if it doesn't exist)
    This function reads all layers in the shapefile directory whose name obeys
    the regex and puts them in a single table.
    """
    table=None
    ses=self.Session()
    # currently we can only cope with updating the entire table as a whole
    self.dropTable(tablename,schema)
    # if self.dbeng.has_table(tablename,schema=schema):
    print("Filling POSTGIS table %s:%s with data from"%(schema,tablename),folder,file=Log)
    #open shapefile directory
    shpf=ogr.Open(folder)
    for il in range(shpf.GetLayerCount()):
        #check for regex
        if regex:
            if not bool(re.search(regex,shpf[il].GetName())):
                continue
        for ift in range(shpf[il].GetFeatureCount()):
            #we need to make a emporary clone here as osgeo will cause a segfault otherwise
            feat=shpf[il][ift].Clone()
            #print(feat.geometry().GetGeometryName(),file=Log)
            if table == None:
                cols=columnsFromFeat(feat,forceGType=forceGType)
                table=Table(tablename,self.mdata,*cols,schema=schema)
                table.create(checkfirst=True)
                tableMap=tableMapFactory(tablename,table)
            values=valuesFromFeat(feat)
            try:
                ses.add(tableMap(**values))
            except:
                pass
    ses.commit()
    # self.vacuumAnalyze(tablename,schema)
    ses.commit()
    ses.close()

