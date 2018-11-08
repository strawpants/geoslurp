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
import re
from datetime import datetime
import os
from geoslurp.datapull import UriBase
from geoslurp.datapull import CrawlerBase

class Uri(UriBase):
    def __init__(self,url,lastmod=None):
        super().__init__(url,lastmod)
        if not bool(re.match('^ftps?://',url)):
            raise Exception("URL does not seem to be a valid ftp(s) address")

class Crawler(CrawlerBase):
    """Crawler for ftp directories"""
    def __init__(self,url,pattern):
        super().__init__(url)
        self.pattern=pattern

    def uris(self, check=False):
        """Generate a list files in a directory and return a list of uri"""
        regexdate=re.compile(b'((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[0-9]{2} +[0-9]{4})')

        regexthisyear=re.compile(b'((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[0-9]{2} +[0-9]{2}:[0-9]{2})')
        buf=Uri(self.rooturl).buffer()
        for ln in buf.getvalue().splitlines():
            fname=ln.decode('utf-8').split()[-1]
            #only apply the pattern to the last column
            tmp=re.search(self.pattern,fname)
            if tmp:
                t=None
                #try to parse the date from the buffer line
                match=regexdate.search(ln)
                if match:
                    t=datetime.strptime(match.group(1).decode('utf-8'),'%b %d %Y')
                else:
                    #try to see if the date contains HH:MM (this year)
                    match=regexthisyear.search(ln)
                    if match:
                        t=datetime.strptime(match.group(1).decode('utf-8'),'%b %d %H:%M')
                        t=t.replace(year=datetime.now().year)

                uri=Uri(os.path.join(self.rooturl,fname),lastmod=t)

                if uri.lastmod == None:
                    #one can try to get this information through the header informatio too (slower and not always working)
                    uri.updateModTime()
                yield uri

