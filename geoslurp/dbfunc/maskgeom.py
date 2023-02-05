from geoslurp.dbfunc.dbfunc import DBFunc
from geoslurp.config.catalogue import geoslurpCatalogue


class gs_maskgeom(DBFunc):
    """Registers a plpython function which returns a raster made from a geometry"""
    language="plpgsql"
    inargs=["geom geometry, pixsize numeric, gridreg boolean=false"]
    outargs="raster"
    pgbody=["DECLARE\n"\
            "shft float;\n"\
            "BEGIN\n"\
            "IF gridreg THEN\n"\
            "shft=0;\n"\
            "ELSE\n"\
            "shft=pixsize/2;\n"\
            "END IF;\n"\
           "\tRETURN ST_AsRaster(geom,pixsize,-pixsize,shft,shft, '8BUI');\n"\
            "END"]

    #other overload (using a geograpical extent and pixel size)
    inargs.append("geom geometry,west numeric,east numeric,south numeric,north numeric, pixsize numeric")
    pgbody.append("DECLARE\n"\
            "refrast raster;\n"\
            "maskrast raster;\n"\
            "width integer;\n"
            "height integer;\n"
            "BEGIN\n"\
            "width=floor((east-west)/pixsize);\n"
            "height=floor((north-south)/pixsize);\n"
            "refrast=ST_MakeEmptyRaster(width, height, west, north, pixsize);\n"
            "refrast=ST_addband(refrast,'8BUI'::text,0.0);\n"
            "maskrast=ST_asRaster(geom,refrast,'8BUI');\n"
            "\tRETURN ST_Union(rast) as rast from refrast,maskrast;\n"\
            "END")


    def __init__(self,dbcon):
        super().__init__(dbcon)

geoslurpCatalogue.addDbFunc(gs_maskgeom)
#"\tRETURN ST_Union(ST_AsRaster(geom,refrast, '8BUI'),refrast);\n"\
