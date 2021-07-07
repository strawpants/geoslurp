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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2021



import xarray as xr

@xr.register_dataarray_accessor("gslrp")
class XarGeoslurp:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    @property
    def storage(self):
        if "gslrp_storage" in self._obj.attrs:
            return self._obj.attrs["gslrp_storage"]
        else:
            return None

    @storage.setter
    def storage(self,uri):
        self._obj.attrs["gslrp_storage"]=uri
    
    def expand_dim(self,name,value):
        """Expands the data array with a 1-sized dimension possibly adding a coordinate value"""
        da=self._obj.expand_dims({name:1})
        da=da.assign_coords({name:[value]})
        da.gslrp.append_dim=name
        return da

    @property
    def append_dim(self):
        if "gslrp_append_dim" in self._obj.attrs:
            return self._obj.attrs["gslrp_append_dim"]
        else:
            return None

    @append_dim.setter
    def append_dim(self,append_dim):
        self._obj.attrs["gslrp_append_dim"]=append_dim

    def join_at(self,**kwargs):
        """When reading/writing to geoslurp, join this datarray with others at this dimension/coordinate"""
        if len(kwargs) != 1:
            raise TypeError( "connectAt only accepts one argument)")

        for ky,val in kwargs.items():
            if ky in self._obj.coords:
                pass
            else:
                #add a new dimensions and coordinate (dim and coord have the same name)
                self.append_dim=ky
                da=self._obj.expand_dims({ky:1})
                da=da.assign_coords({ky:[val]})
        return da
