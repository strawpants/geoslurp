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


from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import os
class CrawlerBase(ABC):
    rooturl=None
    def __init__(self,url):
        self.rooturl=url

    @abstractmethod
    def uris(self):
        """Generator which returns uri's to requested datasets"""
        pass

    def parallelDownload(self,outdir,check=False,maxconn=8,gzip=False,continueonError=False):
        """
        Download uris in parallel
        :param direc: directory to download to
        :param check: Only download when newer or non-existent (default to False)
        :param maxconn: amount of parallel downloads to execute
        :param continueOnError (bool): keep trying
        """
        """Download/Update files in a given directory (returns a list of updated files)
        Note the download is executed in parallel"""


        updated=[]
        futures=[]
        with ThreadPoolExecutor(max_workers=maxconn) as connectionPool:
            for uri in self.uris():
                # print("add ",uri.url)
                futures.append(connectionPool.submit(deepcopy(uri).download,direc=outdir,check=check,gzip=gzip,continueonError=continueonError))

        for future in futures:
            if future.result()[1]:
                updated.append(future.result()[0])

        return updated
