#author alisa yakhontova (yakhontova@geod.uni-bonn.de) 2020

from netCDF4 import Dataset
import numpy as np
from netCDF4 import num2date
from datetime import datetime

def sec2month(nc_id):
    """convert seconds since.. to yyyy-mm-dd format"""
    times=nc_id["time"]
    units=times.units
    calendar=times.calendar
    tt=[entry for entry in num2date(times[:],units)]
     
    return tt

def getTemp(fesomURI,tspan,zlvlid):
    temp_all=[]
    #loop over single files 
    for i in range(len(fesomURI)):
        with Dataset(fesomURI[i]["uri"], 'r') as nc_id:  # open file and obtain its id
            time=sec2month(nc_id)
            #loop over inside the file
            for j, t in enumerate(time):
                if tspan[0] <= t and t < tspan[1]:
                    temp=np.array(nc_id["temp"][j,zlvlid])
                    if len(temp_all)==0:
                        temp_all=[temp]
                    else: 
                        temp_all = np.concatenate((temp_all, [temp])) 
    return temp_all
