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
# Schema which stores Argo data (links)
from geoslurp.schema import Schema

class Argo(Schema):
    """A scheme to download and manage Argo float data"""
    def __init__(self):
        self._catalog='http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS/nmdis/2901631/profiles/catalog.xml'




#from netCDF4 import Dataset

# catalog='http://tds0.ifremer.fr/thredds/catalog/CORIOLIS-ARGO-GDAC-OBS/nmdis/2901631/profiles/catalog.xml'
# furl='http://tds0.ifremer.fr/thredds/dodsC/CORIOLIS-ARGO-GDAC-OBS/nmdis/2901632/profiles/R2901632_156.nc'
# ncid=Dataset(furl)
 
