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

from geoslurp.datapull import UriBase,CrawlerBase
from geoslurp.datapull.http import Uri as http
from html.parser import HTMLParser
import re

class icgemParser(HTMLParser):
    entryActive=False
    rowregex=re.compile('(^tom-row(?!-header))|(^tom-row-odd)')
    entry={}
    models=[]
    dataname=None
    linkname=None
    def __init(self,rooturl):
        pass
    def handle_starttag(self, tag, attrs):
        if tag == 'tr' and self.rowregex.match(attrs[0][1]):
            self.entry={}
            self.entryActive=True

        if not self.entryActive:
            return

        if tag == 'a' and self.linkname:
            if attrs[0][0] =='href':
                self.entry[self.linkname]=attrs[0][1]
                self.linkname=None

        if tag =='td' and attrs[0][1] == 'tom-cell-name':
            self.dataname="name"
            self.linkname="reference"

        if tag =='td' and attrs[0][1] == 'tom-cell-doilink':
            #note this will overwrite a previous reference link (i.e. doi is preferred)
            self.linkname="reference"

        if tag =='td' and attrs[0][1] == 'tom-cell-year':
            self.dataname="year"

        if tag =='td' and attrs[0][1] == 'tom-cell-degree':
            self.dataname="nmax"

        if tag =='td' and attrs[0][1] == 'tom-cell-modelfile':
            self.linkname="url"

    def handle_endtag(self, tag):
        if self.entryActive and tag == 'tr':
            self.entryActive=False
            self.models.append(self.entry)
            self.entry={}

    def handle_data(self, data):
        if self.entryActive and self.dataname:
            if data.strip() == '':
                return
            self.entry[self.dataname]=data
            #reset dataname after entering data int he dict
            self.dataname=None


class Uri(UriBase):
    """Holds an uri to an icgem static field"""
    def __init__(self,url,lastmod=None):
        super().__init__(url,lastmod)

class Crawler(CrawlerBase):
    """Crawl icgem static fields"""
    def __init__(self):
        super().__init__(url="http://icgem.gfz-potsdam.de/tom_longtime")

    def uris(self):
        """List uris of available static models"""
        buf=http("http://icgem.gfz-potsdam.de/tom_longtime").buffer()
        parser=icgemParser()
        parser.feed(buf.getvalue().decode('utf-8'))
        for dct in parser.models:
            print(dct)

