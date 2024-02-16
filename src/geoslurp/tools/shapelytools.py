# This file is part of geoslurp-tools.
# geoslurp-tools is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# geoslurp-tools is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019


from rasterio.io import MemoryFile
import shapely.wkb

def shpextract(entry):
    """ extract a shapely object from the database entry"""
    return shapely.wkb.loads(str(entry['geom']), hex=True)


# def closestDistance():
def wkb2shapely(geom):
    return shapely.wkb.loads(str(geom),hex=True)

def gdal2rastio(rast):
    return MemoryFile(rast.tobytes())
