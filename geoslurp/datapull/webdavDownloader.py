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

import easywebdav
class WebdavProvider():
    """
    Get files from a webdav capable server
    """
    def __init__(self, rooturl, user=None, passw=None):
        """
        Initialize the webdav
        :param rooturl:  top url to start off from
        :param user: Username
        :param passw: password
        """

        #extract protocol from url
        proto,url=rooturl.split('://')
        #also strip off direectory
        url, direc=url.split('/', 1)
        self.webdav = easywebdav.connect(url, username=user, password=passw, protocol=proto)
        #change directory
        self.webdav.cd(direc)
    def ls(self,arg='.'):
        return self.webdav.ls(arg)

    def updateFiles(self,outdir,pattern,log=None):
        """
        Download files but only update those which have newer remote versions
        :param outdir:
        :param pattern:
        :param log:
        :return:
        """
        pass