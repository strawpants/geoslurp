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

from geoslurp.dataset import DataSet
from geoslurp.datapull import ThreddsConnector,ThreddsFilter,getDate
from geoalchemy2.types import Geography
from geoalchemy2.elements import WKBElement
from sqlalchemy import Column,Integer,String, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from netCDF4 import Dataset as ncDset
from osgeo import ogr
from datetime import datetime,timedelta
# To do:  etract meta information with a threadpool
#from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.ext.declarative import declarative_base

# A declarative base which can be used to create database tables

OceanObsTBase=declarative_base(metadata=MetaData(schema='oceanobs'))

# Setup the postgres table

geoPointtype = Geography(geometry_type="POINTZ", srid='4326', spatial_index=True,dimension=3)

class ArgoTable(OceanObsTBase):
    """Defines the Argo PostgreSQL table"""
    __tablename__='argo'
    id=Column(Integer,primary_key=True)
    datacenter=Column(String)
    lastupdate=Column(TIMESTAMP)
    tprofile=Column(TIMESTAMP)
    tlocation=Column(TIMESTAMP)
    wmoid=Column(Integer)
    cycle=Column(Integer)
    uri=Column(String)
    mode=Column(String)
    profnr=Column(Integer)
    ascending=Column(Boolean)
    geom=Column(geoPointtype)
    data=Column(JSONB)

def ncStr(ncelem):
    """extracts a utf-8 encoded string from a  netcdf character variable and strips trailing junk"""
    return b"".join(ncelem).decode('utf-8').strip("\0")

def argoMetaExtractor(uri):
    """Extract meta information as a drictionary from  an argo floats
    Each registered profile gets a separate entry"""

    meta=[]

    ncArgo=ncDset(uri)
    # Get reference time
    t0=datetime.strptime(b"".join(ncArgo["REFERENCE_DATE_TIME"][:]).decode("utf-8"),"%Y%m%d%H%M%S")
    for iprof in range(ncArgo.dimensions["N_PROF"].size):
        # geographical location
        geoLoc=ogr.Geometry(ogr.wkbPoint)
        geoLoc.AddPoint(float(ncArgo["LATITUDE"][iprof]), float(ncArgo["LONGITUDE"][iprof]))
        # time point
        tprof=t0+timedelta(days=float(ncArgo["JULD"][iprof].data))
        tloc=t0+timedelta(days=float(ncArgo["JULD_LOCATION"][iprof].data))
        cycle=int(ncArgo["CYCLE_NUMBER"][iprof].data)
        datacenter=ncStr(ncArgo["DATA_CENTRE"][iprof])
        direction=bool(ncArgo["DIRECTION"][iprof].data == b"A")
        wmoid=int(ncStr(ncArgo["PLATFORM_NUMBER"][iprof]))
        mode=ncStr(ncArgo["DATA_MODE"][:])[iprof]
        meta.append({"datacenter":datacenter,"lastupdate":datetime.now(), "tprofile":tprof,
                     "tlocation":tloc, "wmoid":wmoid, "cycle":cycle , "uri":uri,
                     "mode":mode, "profnr":iprof, "ascending":direction,
                     "geom":WKBElement(geoLoc.ExportToWkb(),srid=4326,extended=True),"data":{}})

    return meta


class Argo(DataSet):
    """Argo table"""
    def __init__(self,scheme):
        super().__init__(scheme)

        # Create table if it doesn't exist
        # import pdb;pdb.set_trace()
        # OceanObsTBase.metadata.create_all(self.scheme.db.dbeng, tables=[ArgoTable],checkfirst=True)
        OceanObsTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)
        self._uris=[]


    def pull(self):
        """Get a list of netcdf files from the Ifremer opendap Thredds server"""

        conn=ThreddsConnector("http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS/catalog.xml", followfilter=ThreddsFilter("dataset").OR("catalogRef", attr="ID", regex=".*aoml.*"))
        i=0
        for ds in conn.items():
            uri=conn.rooturl+"/"+conn.services.opendap+'/'+ds.attrib['urlPath']
            # check if it needs updating
            lastmod=getDate(ds)

            # possibly check whether we actually need an update

            self._uris.append(uri)
            # for metadict in argoMetaExtractor(conn.services.opendap+"/"+ds.attrib["urlPath"]):
            i+=1
            if i > 2:
                break

    def register(self):
        """Extracts metadat from the float and registers it in the database"""
        #create a database session
        ses=self.scheme.db.Session()

        for uri in self._uris:
            for metadict in argoMetaExtractor(uri):
                entry=ArgoTable(**metadict)
                ses.add(entry)

        ses.commit()

        #also update the inventory
        # self._inventData

    def purge(self):
        pass


