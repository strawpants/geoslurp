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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplog
import re
import pandas as pd





class PandasBase(DataSet):
    """Base class which reads in a pandas compatible table (CSV, excel are currently supported) it in a db table"""
    table=None
    pdfile=None
    skipfooter=0
    ftype="csv"
    dtypes=None
    encoding=None
    def __init__(self,dbconn):
        super().__init__(dbconn)
    def modify_df(self,df):
        """A derived type can overload this to make modifications to the dataframe before registering it in the database"""
        return df

    def register(self):
        """Update/populate a database table from a pandas compatible file) 
    """
        if self.ftype == "csv":
            df=pd.read_csv(self.pdfile,skipfooter=self.skipfooter,encoding=self.encoding)
        elif self.ftype == "excel":
            df=pd.read_excel(self.pdfile,skipfooter=self.skipfooter)

        else:
            raise RuntimeError("Don't know how to open %s, specify ftype"%(self.pdfile))
            #possibly modify dataframe in derived class 
            
        slurplog.info("Filling pandas table %s.%s with data from %s" % (self.scheme, self.name, self.pdfile))
        
        df=self.modify_df(df)                
        df.to_sql(self.name,self.db.dbeng,schema=self.scheme, if_exists='replace', dtype=self.dtypes)


        #also update entry in the inventory table
        self.updateInvent()

