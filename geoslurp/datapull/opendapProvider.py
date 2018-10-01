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
from geoslurp.datapull import httpProvider
from io  import BytesIO
import re
import os

class OpendapFilter():
    """Helper class to aid traversing to opendap xml elements"""
    def __init__(self,xmltyp="*",attr=None,regex=None):
        """Sets up a filter. Note that the default xmltype of '*' accepts all elements"""
        self._type=xmltyp
        self._orFilter=None
        self._andFilter=None
        self._attr=attr
        if regex:
            self._regex=re.compile(regex)

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
                    valid=False
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
        self._orFilter=OpendapFilter(xmltyp, attr, regex)
        return self

    def AND(self, xmltyp, attr=None, regex=None):
        """Provides a method for chaining OR filters"""
        if self._orFilter:
            raise ValueError("Cannot apply AND and OR at the same time")
        self._andFilter=OpendapFilter(xmltyp, attr, regex)
        return self



def gethref(input):
    """small function to extract a href link from a dictionary"""
    for ky,val in input.items():
        if ky.endswith("href"):
            return val
    return None

class OpendapConnector:
    """A class to work with an Opendap server"""
    def __init__(self, catalogurl, filter=OpendapFilter("dataset",attr="urlPath"), followfilter=OpendapFilter("catalogRef").OR("dataset")):
        #load the root catalog
        self._rootxml=self.getCatalog(catalogurl)
        self._baseurl=os.path.dirname(catalogurl)
        self._filt=filter
        self._followFilt=followfilter

    @staticmethod
    def getCatalog(url):
        """Retrieve a catalogue"""
        print("getting Opendap catalog: %s"%(url))
        buf=BytesIO()
        http=httpProvider(url)
        http.downloadFile(buf)
        # print(buf.getvalue())
        return XMLTree.fromstring(buf.getvalue())


    def getopendapRoot(catalog):
        """Retrieves the root opendap url from a catalogue"""
        import pdb;pdb.set_trace()
        for xml in catalog:
            if xml.tag.endswith('service') and re.match("Compound",xml.attrib["serviceType"]):
                return xml.attrib["base"]

    @staticmethod
    def gethttpRoot(catalog):
        """Retrieves the root opendap url from a catalogue"""
        for elem in catalog.findall("service"):
            if re.match("HTTPServer",elem.attrib["serviceType"]):
                return elem.attrib["base"]

    def items(self,xmlcatalog=None ,depth=10):
        """Generator which returns xml nodes which obey a certain filter
        Nodes which obey the followFilter will be recursively searched"""

        if depth == 0:
            # signals a stopiteration
            return
        else:
            depth-=1

        if xmlcatalog is None:
            xmlcatalog=self._rootxml
        for xelem in xmlcatalog:

            if self._filt.isValid(xelem):
                # Allright we can return this entry straight away
                yield xelem
                # Also continue with the loop after yielding
                continue

            if self._followFilt.isValid(xelem):
                # If this is the case we may need a recursive search in either a
                if xelem.tag.endswith("CatalogRef"):
                    # We treat CatalogRefs in a special way by retrieving the subcatalog from the OpenDap server
                    subxml=self.getCatalog(self._baseurl+"/"+gethref(xelem))
                else:
                    # Otherwise we're just going to look in the children of the current element
                    subxml=xelem
                yield from self.items(subxml, depth)

