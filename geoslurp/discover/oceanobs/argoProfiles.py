# author alisa yakhontova (yakhontova@geod.uni-bonn.de) 2020
from netCDF4 import Dataset
import numpy as np
from netCDF4 import num2date

def sec2month(nc_id):
    """convert seconds since.. to yyyy-mm-dd format"""
    times=nc_id["time"]
    units=times.units
    # calendar=times.calendar
    tt=[entry for entry in num2date(times[:],units)]
     
    return tt


