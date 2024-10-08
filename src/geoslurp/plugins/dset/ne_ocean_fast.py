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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2024

from geoslurp.dataset import DataSet
from geoslurp.config.catalogue import DatasetCatalogue
from geoslurp.config.slurplogger import slurplog
from sqlalchemy import text

class NE_10m_oceanfunc(DataSet):
    schema='natearth'
    required="natearth.ne_10m_ocean"
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
    def pull(self):
        geoslurpCatalogue=DatasetCatalogue()
        #retrieve the required natural Earth Dataset
        oceanbase=geoslurpCatalogue.getDsetClass(self.conf, self.required)(self.db)
        if oceanbase.exists:
            #no need to pull and register
            return
        oceanbase.pull()
        oceanbase.register()
    def register(self):
        self.dropTable()
        #create the subdivided and indexed ocean function
        stname=self.stname()

        sqlcommands=f"""CREATE TABLE {stname} AS SELECT ST_subdivide(ST_makevalid(geom)) as geom FROM (SELECT (ST_dump(geom::geometry)).geom AS geom FROM {self.required}) as mpoly where ST_area(mpoly.geom) > 100;
        CREATE INDEX {self.tname()}_geom_idx on {stname} USING GIST(geom);
        """
        
        slurplog.info(f"creating table {stname} and index, this can take a while..")
        self._ses.execute(text(sqlcommands))
        self._ses.commit()
        slurplog.info("Done..")

        self.updateInvent()


def getOceanFastDsets(conf):
    return [NE_10m_oceanfunc]
