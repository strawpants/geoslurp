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
from lxml.etree import HTML as HTMLtree
import os
from datetime import datetime

class Uri(UriBase):
    """Holds an uri to an icgem static field"""
    def __init__(self,url,lastmod=None,name=None,ref=None,nmax=None,year=None):
        if year and not lastmod:
            #use year as the last modification time
            lastmod=datetime(year,12,31)
        super().__init__(url,lastmod)
        self.name=name
        self.ref=ref
        self.nmax=nmax


class Crawler(CrawlerBase):
    """Crawl icgem static fields"""
    def __init__(self):
        super().__init__(url="http://icgem.gfz-potsdam.de/tom_longtime")
        buf=http(self.rooturl).buffer()
        self._roothtml=HTMLtree(buf.getvalue())

    def uris(self):
        """List uris of available static models"""

        rowregex=re.compile('(^tom-row(?!-header))|(^tom-row-odd)')
        for elem in self._roothtml.iterfind('.//tr'):
            uridict={}
            if not rowregex.match(elem.attrib['class']):
                continue

            nameelem=elem.find(".//td[@class='tom-cell-name']")
            if nameelem.text.strip() != '':
                #just find the name end strip line ending
                uridict["name"]=nameelem.text.lstrip()[:-1]
            else:
                #find a name and reference
                nameelem=nameelem.find(".//a[@href]")
                uridict["name"]=nameelem.text
                uridict["ref"]=nameelem.attrib['href']

            #find the year, maximum degree, doi etc
            uridict["year"]=int(elem.find(".//td[@class='tom-cell-year']").text)
            uridict["nmax"]=int(elem.find(".//td[@class='tom-cell-degree']").text)
            try:
                uridict["url"]=os.path.dirname(self.rooturl)+elem.find(".//td[@class='tom-cell-modelfile']").find(".//a[@href]").attrib["href"]
            except AttributeError:
                #not avaailable for download so skip this entry
                continue

            try:
                uridict["ref"]=elem.find(".//td[@class='tom-cell-doilink']").find(".//a[@href]").attrib["href"]
            except AttributeError:
                #no problem as this entry is optional just pass
                pass
            yield Uri(**uridict)
