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


def addRemove(parser):
	parser.add_argument('--remove',action='store_true',help='remove selected datasets in this datasource')

def addForce(parser):
	parser.add_argument('--force',action='store_true',help='enforce action')

def addUpdate(parser):
    parser.add_argument('--update',action='store_true',help='download/register selected datasets')

def addDownload(parser):
    parser.add_argument('--download',action='store_true',help='only download selected datasets to cache')

def addRegister(parser):
    parser.add_argument('--register',action='store_true',help='register selected datasets')

commonOptions={"remove":addRemove,"force":addForce,"update":addUpdate,"download":addDownload,"register":addRegister}
