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


from geoslurp.dataset import DataSet
from geoslurp.datapull import WebdavProvider

class PODAACBase(DataSet):
    """A base class for datasets which can be downloaded from the podaac webdav drive"""
    __version__=(0, 0, 0)
    rooturl=None
    pattern=None
    def __init__(self,scheme):
        super().__init__(scheme)
        # self.webdav=WebdavProvider(self.url,user=self.user,passw=self.password)
