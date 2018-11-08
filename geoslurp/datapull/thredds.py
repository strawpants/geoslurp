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

from lxml import etree as XMLTree
# from geoslurp.datapull import httpProvider
from geoslurp.datapull import UriHttp as http
from io  import BytesIO
import re
import os
from geoslurp.config import Log
from collections import namedtuple
from datetime  import datetime
from dateutil.parser import parse as isoParser

from geoslurp.datapull import BaseCrawler

class ThreddsFilter():
    """Helper class to aid traversing to opendap xml elements"""
    def __init__(self,xmltyp="*",attr=None,regex=None):
        """Sets up a filter. Note that the default xmltype of '*' accepts all elements"""
        self._type=xmltyp
        self._orFilter=None
        self._andFilter=None
        self._attr=attr
        if regex:
            self._regex=re.compile(regex)
        else:
            self._regex=None

    def isCatalog(self):
        """Check if the filter type is a catalogRef"""
        if self._type == 'catalogRef':
            return True
        else:
            return False

    def isValid(self,xmlelem):
        """Filter xmlelem on  attributes"""

        valid=False
        # First test: Datatype test
        if self._type == "*" or xmlelem.tag.endswith(self._type):
            valid= True

        # Second test: attribute test (but test only when datatype matches)
        if valid and self._attr:
            # Also do an attribute test
            if self._attr in xmlelem.attrib:
                if self._regex:
                    valid=bool(self._regex.match(xmlelem.attrib[self._attr]))
                else:
                    # return True when no regex search is needed
                    valid=True
            else:
                # When the attribute is not found the test is considered as false
                valid=False

        #possibly apply chained OR and AND filters
        if self._orFilter and not valid:
            # perform an additional test
            return self._orFilter.isValid(xmlelem)

        if self._andFilter and valid:
            return self._andFilter.isValid(xmlelem)

        #else return the result from the first 2 tests
        return valid

    def OR(self, xmltyp, attr=None, regex=None):
        """Provides a method for chaining OR filters"""
        if self._andFilter:
            raise ValueError("Cannot apply AND and OR at the same time")
        self._orFilter=ThreddsFilter(xmltyp, attr, regex)
        return self

    def AND(self, xmltyp, attr=None, regex=None):
        """Provides a method for chaining OR filters"""
        if self._orFilter:
            raise ValueError("Cannot apply AND and OR at the same time")
        self._andFilter=ThreddsFilter(xmltyp, attr, regex)
        return self



def gethref(input):
    """small function to extract a href link from a dictionary"""
    for ky,val in input.items():
        if ky.endswith("href"):
            return val
    return None

def getDate(xml):
    """extracts the date from a dataset element"""
    for elem in xml:
        if elem.tag.endswith("date"):
            return isoParser(elem.text)

def getTagEnding(xml):
    """Strip the leading junk ({...}) from a tag"""
    ln=xml.tag.split('}')
    if len(ln) == 1:
        return ln[0]
    else:
        return ln[1]

def getAttrib(xml,regex):
    """Search in xml attributes based on a regex"""
    for ky,val in xml.attrib.items():
        if re.search(regex,ky):
            return val
    return None

class ThreddsCrawler(BaseCrawler):
    """A class to work with an Opendap server"""
    def __init__(self, catalogurl, filter=ThreddsFilter("dataset", attr="urlPath"), followfilter=ThreddsFilter("catalogRef").OR("dataset")):
        super().__init__(url=catalogurl)
        #load the root catalog
        self._rootxml=self.getCatalog(catalogurl)
        self.services=self.getServices(self._rootxml)
        self._filt=filter
        self._followFilt=followfilter

    @staticmethod
    def getCatalog(url):
        """Retrieve a catalogue"""
        print("getting Opendap catalog: %s"%(url),file=Log)
        buf=BytesIO()
        cathttp=http(url)
        http.downloadFile(buf)
        # print(buf.getvalue())
        return XMLTree.fromstring(buf.getvalue())


    @staticmethod
    def getServices(catalog,depth=2):
        """Retrieves the root for serving files over http url from a catalogue"""
        compoundfilt=ThreddsFilter("service", attr="serviceType", regex="Compound")
        opendapfilt=ThreddsFilter("service", attr="serviceType", regex="(OpenDAP)|(OPENDAP)|(DODS)")
        httpfilt=ThreddsFilter("service", attr="serviceType", regex="HTTPServer")
        # possibly add other serices if deemed usefull
        servtuple=namedtuple("Service","opendap http")

        if depth == 0:
            return
        else:
            depth-=1

        for elem in catalog:
            if elem.tag.endswith("service"):

                if opendapfilt.isValid(elem):
                    opendap=elem.attrib["base"]

                if httpfilt.isValid(elem):
                    http=elem.attrib["base"]

                if compoundfilt.isValid(elem):
                    # allow recursion
                    return ThreddsCrawler.getServices(elem, depth)

        return servtuple(opendap=opendap,http=http)


    def items(self,xmlcatalog=None,url=None ,depth=10):
        """Generator which returns xml nodes which obey a certain filter
        Nodes which obey the followFilter will be recursively searched"""

        if depth == 0:
            # signals a stopiteration
            return
        else:
            depth-=1

        if xmlcatalog is None:
            xmlcatalog=self._rootxml
        if url is None:
            url=self._catalogurl

        for xelem in xmlcatalog:

            if self._filt.isValid(xelem):
                # Allright we can return this entry straight away
                yield xelem
                # Also continue with the loop after yielding
                continue

            if not self._followFilt:
                # continue with the next element if no element should be followed
                continue

            if self._followFilt.isValid(xelem):
                # If this is the case we may need a recursive search in either a
                if xelem.tag.endswith("catalogRef"):
                    # We treat CatalogRefs in a special way by retrieving the subcatalog from the OpenDap server
                    suburl=os.path.dirname(url)+"/"+gethref(xelem)
                    try:
                        subxml=self.getCatalog(suburl)
                    except:
                        # Just ignore this catalog entry upon exceptions
                        print("Ignoring failed CatalogRef %s"%(suburl),file=Log)
                        continue
                else:
                    # Otherwise we're just going to look in the children of the current element
                    suburl=url
                    subxml=xelem

                yield from self.items(subxml, suburl, depth)

