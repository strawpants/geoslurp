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

from osgeo import ogr

def lonlat2ogr(lonlat):
    """Create a single OGR polygon from an input lonlat array
    :param  lonlat: list like array with longitude latitude pairs
    :returns a ogr polygon"""
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for lon, lat in lonlat:
        ring.AddPoint(lon, lat)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly

