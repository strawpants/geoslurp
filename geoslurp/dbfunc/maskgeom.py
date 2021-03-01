from geoslurp.dbfunc.dbfunc import DBFunc
from geoslurp.config.catalogue import geoslurpCatalogue


class gs_maskgeom(DBFunc):
    """Registers a plpython function which returns a raster made from a geometry"""
    language="plpgsql"
    inargs="geom geometry, pixsize numeric, gridreg boolean=false"
    outargs="raster"
    pgbody="DECLARE\n"\
            "shft float;\n"\
            "BEGIN\n"\
            "IF gridreg THEN\n"\
            "shft=0;\n"\
            "ELSE\n"\
            "shft=pixsize/2;\n"\
            "END IF;\n"\
           "\tRETURN ST_AsRaster(geom,pixsize,-pixsize,shft,shft, '8BUI');\n"\
            "END"

    def __init__(self,dbcon):
        super().__init__(dbcon)

geoslurpCatalogue.addDbFunc(gs_maskgeom)
