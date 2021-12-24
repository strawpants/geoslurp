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


from geoslurp.datapull.http import Uri as http
import os
from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.dataset.dataSetBase import DataSet
from geoalchemy2 import Geography,WKTElement
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String,Float
from sqlalchemy.dialects.postgresql import TIMESTAMP,JSONB
from shapely.wkt import dumps as wktdump
from shapely.geometry import Point
import scipy.io
from geoslurp.tools.time import decyear2dt

scheme='oceanobs'

PIESTBase=declarative_base(metadata=MetaData(schema=scheme))

geoPointtype = Geography(geometry_type="POINT", srid='4326', spatial_index=True,dimension=2)

class PIESTable(PIESTBase):
    __tablename__='awipies'
    id=Column(Integer,primary_key=True)
    name=Column(String)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    depth=Column(Float)
    vars=Column(JSONB)
    uri=Column(String)
    geom=Column(geoPointtype)

def extractMetaPies(datafile):
    """Co routine which spits outs meta info per PIES-site"""
    dat=scipy.io.loadmat(datafile)
    names=[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17]
    for i in range(dat["OLENGTH"].shape[1]):
        depth=float(dat["DEPTH"][0][i])
        pnt=Point(dat["degE"][i][0],dat["degN"][i][0])
        varlookup={"OPRES":(i,(0,-1,1))}
        meta={"name":str(names[i]),"depth":depth,"tstart":decyear2dt(dat["OTMIN"][0][i]),
              "tend":decyear2dt(dat["OTMAX"][0][i]),"vars":varlookup,
              "uri":datafile, "geom":WKTElement(wktdump(pnt))}

        yield meta




class awipies(DataSet):
    """Class whichs downloads/register athe AWI South Atlantic PIES"""
    scheme=scheme
    table=PIESTable
    obpfile='OBPv3withtau.mat'
    def __init__(self,dbconn):
        super().__init__(dbconn)
        PIESTBase.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self):
        """Pulls the OBP matlab file from the cloud"""
        cred=self.conf.authCred("awipies",['url','user','passw'])
        obpsource=http(cred.url,auth=cred)
        obpsource.download(self.dataDir(),outfile=self.obpfile)

    def register(self):
        self.truncateTable()
        obpfile=os.path.join(self.dataDir(),self.obpfile)

        for meta in extractMetaPies(obpfile):
            self.addEntry(meta)

        self.updateInvent()


geoslurpCatalogue.addDataset(awipies)
