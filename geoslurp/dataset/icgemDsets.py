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
# provides a dataset and table for static gravity fields from the icgem website



from geoslurp.dataset import DataSet,GravitySHTBase
from geoslurp.datapull.icgem import  Crawler as IcgemCrawler

def icgemMetaExtractor(uri):
    """Extract meta information from a gzipped icgem file"""
    # buf=StringIO()
    # with gzip.open(uri.url,'rt') as fid:
    #     logging.info("Extracting info from %s"%(uri.url))
    #     for ln in fid:
    #         if '# End of YAML header' in ln:
    #             break
    #         else:
    #             buf.write(ln)
    # hdr=yaml.safe_load(buf.getvalue())["header"]
    # nonstand=hdr["non-standard_attributes"]
    #
    #
    meta={}
    # meta={"nmax":hdr["dimensions"]["degree"],
    #       "omax":hdr["dimensions"]["order"],
    #       "tstart":hdr["global_attributes"]["time_coverage_start"],
    #       "tend":hdr["global_attributes"]["time_coverage_end"],
    #       "lastupdate":uri.lastmod,
    #       "format":nonstand["format_id"]["short_name"],
    #       "gm":nonstand["earth_gravity_param"]["value"],
    #       "re":nonstand["mean_equator_radius"]["value"],
    #       "uri":uri.url,
    #       "type":nonstand["product_id"][0:3],
    #       "data":{"description":hdr["global_attributes"]["title"]}
    #       }
    #
    # #add tide system
    # try:
    #     tmp=nonstand["permanent_tide_flag"]
    #     if re.search('inclusive',tmp):
    #         meta["tidesystem"]="zero-tide"
    #     elif re.search('exclusive'):
    #         meta["tidesystem"]="tide-free"
    # except:
    #     pass

    return meta


class ICGEM_static(DataSet):
    """Manages the static gravity fields which are hosted at http://icgem.gfz-potsdam.de/tom_longtime"""
    table=type("ICGEMTable",(GravitySHTBase,), {})
    def __init__(self, scheme):
        super().__init__(scheme)
        #initialize postgreslq table
        GravitySHTBase.metadata.create_all(self.scheme.db.dbeng, checkfirst=True)

    def pull(self):
        """Pulls static gravity fields from the icgem website"""
        crwl=IcgemCrawler()
        crwl.uris()

    def register(self):
        pass


    def halt(self):
        pass

    def purge(self):
        pass

