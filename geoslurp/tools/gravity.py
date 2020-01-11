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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from sqlalchemy import Column,Integer,String, Boolean,Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
import gzip as gz
from geoslurp.config.slurplogger import slurplogger
import re
from datetime import datetime

#define a declarative baseclass for spherical harmonics gravity  data
@as_declarative(metadata=MetaData(schema='gravity'))
class GravitySHTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].lower()
    id = Column(Integer, primary_key=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    nmax=Column(Integer)
    omax=Column(Integer)
    gm=Column(Float)
    re=Column(Float)
    tidesystem=Column(String)
    format=Column(String)
    type=Column(String)
    uri=Column(String, unique=True,index=True)
    data=Column(JSONB)

def icgemMetaExtractor(uri):
    """Extract meta information from a gzipped icgem file"""

    #first extract the icgem header
    headstart=False
    hdr={}
    with gz.open(uri.url,'rt') as fid:
        slurplogger().info("Extracting info from %s"%(uri.url))
        for ln in fid:
            # if "begin_of_head" in ln:
            #     headstart=True
            #     continue

            if headstart and 'end_of_head' in ln:
                break

            # if headstart:
            spl=ln.split()
            if len(spl) == 2:
                hdr[spl[0]]=spl[1]


    try:
        meta={"nmax":int(hdr["max_degree"]),
          "lastupdate":uri.lastmod,
          "format":"icgem",
          "gm":float(hdr["earth_gravity_constant"].replace('D','E')),
          "re":float(hdr["radius"].replace('D','E')),
          "uri":uri.url,
          "type":"GSM",
          "data":{"name":hdr["modelname"]}
          }
    except Exception as e:
        pass

    #add tide system
    try:
        tmp=hdr["tide_system"]
        if re.search('zero_tide',tmp):
            meta["tidesystem"]="zero-tide"
        elif re.search('tide_free',tmp):
            meta["tidesystem"]="tide-free"
    except:
        pass

    return meta


