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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022


import pandas as pd
import geopandas as gpd
import os
from geoslurp.dataset.pandasbase import PandasBase
import shutil

@pd.api.extensions.register_dataframe_accessor("gslrp")
class PdAccessor:
    def __init__(self,pd_obj):
        self._obj=pd_obj

    def save(self,gsconn,tablename,schema="public",overwrite=False,stripuri=False,xrappend_dim=None):
        """Saves a (geo) dataframe to a database engine"""
        TableClass=type(tablename,(PandasBase,),{"scheme":schema,"stripuri":stripuri,"xrappend_dim":xrappend_dim})
        Table=TableClass(gsconn)
        if overwrite:
            zarrar=Table.outdbArchiveName()
            if os.path.exists(zarrar):
                shutil.rmtree(zarrar)
            Table.dropTable()

        Table.register(df=self._obj)

    @staticmethod
    def load(gsconn,qry,index_col=None):
        """Read a dataframe from a database engine and convert to GeoDataFrame when a geometry column is present"""
        #try reading as spatially aware database (postgis, spatialite)
        try:
            df=gpd.read_postgis(qry,gsconn.dbeng,index_col=index_col)
        except: 
            #possible failure when this is not a table which has a geometry column, so try again with reading a non-postgis sql query
            df=pd.read_sql_query(qry,gsconn.dbeng,index_col=index_col)
    
        return df

