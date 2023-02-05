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

from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplog
import re
from sqlalchemy import Table,Column, Integer, String, Float, BigInteger,Date,DateTime, LargeBinary,ARRAY,JSON,BIGINT

from sqlalchemy import func
import numpy as np
from collections import namedtuple
from geoslurp.types.xar import XarDBType
from geoslurp.types.columnmapper import commonMap
import xarray as xr
import pandas as pd
import os
from datetime import datetime


class XarrayBase(DataSet):
    """Base class which allows writing an xarray dataarray/dataset into db table"""
    xarfile=None #xarray compatible datafile
    groupby=None #on which dimension should the xarray be expanded?
    outofdb=False #whether to store the data as a zarr outside of the database
    writeoutofdb=True #set to False to allow the registration of an existing xarray dataset without (re)writing it to disk
    inbulk=False
    timename="time" #possibly convert this datetime coordinate4 variable to a numeric form
    def __init__(self,dbconn):
        super().__init__(dbconn)
    
    # def modifygrp(self,ds):
        # """Modify the dataset before passing it to sqlalchemy custom types"""
        # if not self.outofdb and self.timename in ds:
            # #convert times to a cf compatible numeric value 
            # calendar="proleptic_gregorian"
            # units="Seconds since 1970-01-01"
            # dscpy=ds.copy()
            # cft=date2num([np_to_datetime(dt) for dt in dscpy[self.timename].values],units,calendar)
            # dscpy[self.timename]=cft
            # dscpy[self.timename].attrs.update({"units":units,"calendar":calendar,"long_name":self.timename})
        # else:
            # dscpy=ds
        # import pdb;pdb.set_trace()
        # return dscpy


    def columnsFromXar(self,ds):
        """Returns a  groupby obejct and a list of columns from an xarray object (groupedby)"""
        
        # import pdb;pdb.set_trace() 
        if self.groupby in ds.xindexes:
            indx=ds.get_index(self.groupby)
            if type(indx) == pd.MultiIndex:
                self.mindex=indx
            else:
                self.mindex=None
        else:
            self.mindex=None

        grp=ds.groupby(self.groupby)
        
        cols = [Column('id', Integer, primary_key=True)]

        if self.mindex is None:
            #only one variable to group over
            indexitem1=next(iter(grp.groups))
            ctype=commonMap[type(indexitem1)]
            cols.append(Column(self.groupby,ctype,index=True))
        else:
            # expand found multindex in multiple columns
            for name,indexitem1 in zip(self.mindex.names,self.mindex[0]):
                ctype=commonMap[type(indexitem1)]
                cols.append(Column(name,ctype,index=True))

        #get the first item which is used as an additional index
        #todo: cope with multindices (e.g. index on multiple columns)

        cols.append(Column('data',XarDBType(parentds=ds,outofdb=self.outdbArchiveName(),groupby=self.groupby,writeoutofdb=self.writeoutofdb)))
        return grp,cols

    def registerInDatabase(self,ds):
        self.dropTable()


        grpby,cols=self.columnsFromXar(ds)
        self.createTable(cols)
        
        if self.inbulk:
            bulk=[]
        for grp,val in grpby:
            if self.mindex is None:
                entry={self.groupby:grp,"data":val}
            else:
                entry={ky:item for ky,item in zip(self.mindex.names,grp)}
                entry["data"]=val
            
            if self.inbulk:
                bulk.append(entry) 
            else:
                self.addEntry(entry)

        if self.inbulk:
            self.bulkInsert(bulk)

    def register(self,ds=None):
        """Update/populate a database table from a xarray compatible file or from a dataset directly)"""
        
        if self.xarfile != "" and ds is None:
            #note if a ds is explicitly provided it takes precedence over the data from xarfile
            try:
                ds=xr.open_dataset(self.xarfile,chunks='auto')
            except:
                raise RuntimeError(f"Cannot open Xarray file {self.xarfile}")
        slurplog.info("Filling xarray table %s.%s" % (self.scheme, self.name))
        
        self.registerInDatabase(ds)

        # #also update entry in the inventory table
        self.updateInvent()

    def pull(self):
        """overload when needed"""
        pass


    def outdbArchiveName(self):
        if self.outofdb:
            if self.writeoutofdb:
                return os.path.join(self.dataDir(),self.name+"_data.zarr")
            else:
                return self.xarfile
        else:
            return None
