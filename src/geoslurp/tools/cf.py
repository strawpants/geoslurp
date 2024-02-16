# This file is part of frommle2.
# frommle2 is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# frommle2 is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with Frommle; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022
import os
from datetime import datetime
import requests
from xml.dom import minidom
import yaml
from geoslurp.config.slurplogger import slurplog
from cftime import date2num
from pyproj import CRS

def cf_load(cf_convfile=None):
    if cf_convfile is None:
        cf_convfile=os.path.join(os.path.expanduser('~'),'.cf-conventions.yaml')
    if os.path.exists(cf_convfile):
       slurplog.info(f"Reading CF convention defaults from {cf_convfile}")        
       with open(cf_convfile,'r') as fid:
           cfconv=yaml.safe_load(fid)
    else:
        #create a new version
        resp=requests.get("https://cfconventions.org/Data/cf-standard-names/79/src/cf-standard-name-table.xml")
        user=os.environ["USER"]
        cfconv={"Conventions":"CF-1.9","institution":f"{user}@unknown, Institute, Country","source":"geoslurp"}

        
        cf=minidom.parseString(resp.text)
        cfconv["standard_names"]={un.parentNode.getAttribute("id"):{"units":un.firstChild.data} for un in cf.getElementsByTagName('canonical_units')if un.firstChild is not None}
       #cache to file to file 
        slurplog.info(f"Writing CF convention defaults to {cf_convfile}, modify the file to add better defaults when writing files")        
        with open(cf_convfile,'w') as fid:
            yaml.dump(cfconv, fid, default_flow_style=False)
    
    return cfconv
    
cfdefaults=cf_load()

def cfadd_global(ds,title=None,comment="Auto generated",references="",source=None,crs=None,update=False):
    if not update:
        ds.attrs={}
    ds.attrs['Conventions'] = cfdefaults["Conventions"]
    if title:
        ds.attrs['title'] = title

    ds.attrs['institution'] = cfdefaults["institution"]
    if source:
        ds.attrs['source'] = source
    else:
        ds.attrs['source'] = cfdefaults["source"]
    ds.attrs['history'] = str(datetime.utcnow()) + ' geoslurp'
    ds.attrs['references'] = references
    ds.attrs['comment'] = comment
    if crs:
        crs=CRS(crs)
        ds.attrs['spatial_ref']=crs.to_cf()


def cfadd_standard_name(dsvar,standard_name,units=None,long_name=None):
    if standard_name in cfdefaults["standard_names"]:
        dsvar.attrs=cfdefaults["standard_names"][standard_name]
        dsvar.attrs["standard_name"]=standard_name
    else:
        raise keyError("requested CF standard_name not found")

    if units:
        #overwrite unit
        dsvar.attrs['units']=units

    if long_name:
        dsvar.attrs["long_name"]=long_name

def cfadd_var(dsvar,units=None,long_name=None):
    if units:
        #overwrite unit
        dsvar.attrs['units']=units

    if long_name:
        dsvar.attrs["long_name"]=long_name

def cfadd_coord(dsvar,xyzt,standard_name=None,units=None,long_name=None):
    if xyzt not in "XYZT":
        raise keyError(f"requested axis is not one of 'X','Y','Z','T'")
    
    if standard_name is not None:
        cfadd_standard_name(dsvar,standard_name,units=units,long_name=long_name)
    else:
        cfadd_var(dsvar,units=units,long_name=long_name)

    #add the axis attribute
    dsvar.attrs["axis"]=xyzt

def cfencode_time(dsvar,calendar="proleptic_gregorian",units="seconds since 1970-01-01 00:00:00"):
    dsvar[:]=date2num(dsvar[:],units,calendar)
    dsvar.attrs["units"]=units
    dsvar.attrs["calendar"]=calendar
    dsvar.attrs["axis"]="T"

