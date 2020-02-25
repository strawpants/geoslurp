from netCDF4 import Dataset
import numpy as np
from netCDF4 import num2date

def sec2month(nc_id):
    """convert seconds since.. to yyyy-mm-dd format"""
    times=nc_id["time"]
    units=times.units
    calendar=times.calendar
    month=[entry.month for entry in num2date(times[:],units,calendar=calendar)]
    year=[entry.year for entry in num2date(times[:],units,calendar=calendar)]
    tt=[]
    for j in range(len(month)):
        if int(month[j])<10:
            tt.append(str(int(year[j])) + '-0' + str(int(month[j])) + '-01')
        else: 
            tt.append(str(int(year[j])) + '-' + str(int(month[j])) + '-01')
      
    return tt

# def getprofiles(ncfile,var,tspan,surfaceids):
#     """Generator which extracts profiles for a certain variable from a fesom netcdf file"""
#     with Dataset(ncfile,'r') as nc:
#         # extract valid time nodes
#         nc["time"]
#         #loop over time
        
#         #loop over nodes

def getTemp(fesomURI,tspan,zlvlid):
    temp_all=[]
    #loop over single files 
    for i in range(len(fesomURI)):
        nc_id = Dataset(fesomURI[i]["uri"], 'r')  # open file and obtain its id
        with nc_id as nc:
            # extract valid time nodes
            times=nc_id["time"]
            units=times.units
            calendar=times.calendar
            firstdate=num2date(times[0],units,calendar=calendar)
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