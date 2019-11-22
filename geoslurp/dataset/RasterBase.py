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
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplogger
from geoslurp.datapull.uri import findFiles, UriFile
from sqlalchemy import Column,Integer,String,Float
from geoalchemy2 import Raster
from sqlalchemy import func,select,text
from osgeo import gdal

class RasterBase(DataSet):
    """Base class to load raster (tiles) into the postgis database"""
    srid=None #will try to find out the srid autmatically (but it's better to explicitly set this)
    srcdir=None
    rastregex=".*"
    auxcolumns=None
    outofdb=False
    regularblocking=False
    tiles=None
    srid=4326 #default but can be overruled
    bandname=None
    overviews=None
    #[ulx,xres,xskew,uly,yskew,yres]
    geotransform=None
    def __init__(self,dbcon):
        super().__init__(dbcon)
        if self.outofdb and not self.srcdir:
            #expects stuff in the datadirectory
            self.srcdir=self.dataDir()
        elif not self.outofdb and not self.srcdir:
            #use the cacheDir
            self.srcdir=self.cacheDir()


    def columns(self):
        #construct the columns
        cols=[Column("id",Integer,primary_key=True),Column("uri",String),Column("add_offset",Float),Column("scale_factor",Float)]
        # possibly add auxiliary columns
        if self.auxcolumns:
            cols.extend(self.auxcolumns)
        #add raster column
        cols.append(Column("rast",Raster))
        return cols

    def register(self):
        """Checks the directory for updated raster files and updates them in the database"""
        #find all relevant files
        newfiles=[UriFile(file) for file in findFiles(self.srcdir,self.rastregex)]
        self.dropTable()
        if self.tiles:
            #expand a single raster in tiles
            if len(newfiles) != 1:
                raise RuntimeError("Don't know how to tile multiple input rasters")
            #possibly create a temporary table for storing the complete raster
            # we need to get a session which is bound to a single transaction unit so we can make use of temporay tables
            trans,ses=self.db.transsession()

            tmptable=self.db.createTable("tmp_raster",[Column("id",Integer,primary_key=True),Column("rast",Raster(spatial_index=False))],temporary=True,bind=ses.get_bind())
            meta=self.rastExtract(newfiles[0])
            entry=tmptable(rast=meta["rast"])
            ses.add(entry)
            # create the table but bind it to this session
            self.createTable(self.columns(),session=ses)
            # fill the new table from the tiles
            qry=select([func.ST_Tile(tmptable.rast,self.tiles[0],self.tiles[1]).label('rast')])
            ses.execute(self.table.__table__.insert().from_select(['rast'],qry))
            del meta["rast"]
            #also set the other column data
            if meta:
                ses.execute(self.table.__table__.update().values(**meta))
            #submit transaction
            trans.commit()
        else:

            for uri in newfiles:
                meta=self.rastExtract(uri)
                if not meta:
                    #don't register empty entries
                    continue

                if self.table == None:
                    #create the table when it does not exist
                    self.createTable(self.columns())

                self.addEntry(meta)
            self._ses.commit()

        #fix the srid
        if self.srid:
            self._ses.execute(
                text("select UpdateRasterSRID('%s'::name,'%s'::name,'rast'::name,%d)"%(self.scheme,self.name,self.srid))
            )

        #add/compute raster constraints
        self._ses.execute(
            text("select AddRasterConstraints('%s'::name,'%s'::name,'rast'::name)"%(self.scheme,self.name))
        )
        if self.regularblocking:
            self._ses.execute(
                text("select AddRasterConstraints('%s'::name,'%s'::name,'rast'::name,'regular_blocking')"%(self.scheme,self.name))
            )

        #create overviews
        if self.overviews:
            for factor in self.overviews:
                self._ses.execute(text("select ST_CreateOverview('%s.%s'::regclass, 'rast', %d, 'Lanczos')"%(self.scheme,self.name,factor)))

        self.updateInvent()

    def rastExtract(self,uri):
        """How things are extracted from the raster file (this may be overloaded in derived classes for more granular access"""
        slurplogger().info("Extracting info from raster: %s"%(uri.url))

        bandnr=1
        if self.outofdb:
            #explicitly open the gdal file to get the bounding box info
            if uri.url.endswith(".nc"):
                prefix="NETCDF:"
            else:
                prefix=""

            if self.bandname:
                suffix=":"+self.bandname
            else:
                suffix=""

            fraster=gdal.Open(prefix+uri.url+suffix)

            # if self.geotransform:
                # fraster.SetGeoTransform(self.geotransform)
                # nx=fraster.RasterYSize
                # ny=fraster.RasterXSize
            # else:
            nx=fraster.RasterXSize
            ny=fraster.RasterYSize

            ulx, xres, xskew, uly, yskew, yres  = fraster.GetGeoTransform()
            # uly, yres, yskew, ulx, xskew, xres  = fraster.GetGeoTransform()


            emptyrast=func.ST_MakeEmptyRaster(nx,ny,ulx,uly,xres,yres,xskew,yskew,self.srid)

            #create an out of db rasterband
            try:
                #possibly we require a change of the path when we run postgresql in a container
                outdbfile=uri.url.replace(self.conf["DataDir"],self.conf["InternalDataDir"])
            except:
                outdbfile=uri.url

            rastb=fraster.GetRasterBand(bandnr)
            nodata=rastb.GetNoDataValue()
            scale=rastb.GetScale()
            offset=rastb.GetOffset()


            meta={"rast":func.ST_AddBand(emptyrast,prefix+outdbfile+suffix,[bandnr],0,nodata),
                  "uri":uri.url,"add_offset":offset,"scale_factor":scale}
            fraster=None
        else:
            #read directly from gdal format
            with open(uri.url,'rb') as fid:
                fbytes=fid.read()
                meta={"rast":func.ST_FromGDALRaster(fbytes,srid=self.srid),"uri":uri.url}

        return meta

