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
    """Helper class to aid traversing to an opendap xml elements"""
    def __init__(self,xmltyp="dataset",attr=None,regex=None):
        """Sets up a filter"""
        self._filt=None
        self._type=xmltyp
        if attr and regex:
           self.addFilter(attr, re.compile(regex))

    def isType(self, xmlelem):
        if xlmelem.tag.endswith(self._type):
            return True
        else:
            return False

    def isValid(self,xmlelem):
        """Filter xmlelem on  attributes"""
        if not xmlelem.tag.endswith(self._type):
            # Quick return if this filter does not apply to this type
            return False

        if self._filt:
            # possibly reject when filter is not applicable
            valid=False
            for ky,val in self._filt.items():
                if ky in xmlelem.attrib:
                    # import pdb;pdb.set_trace()
                    "Only test when the attribute is present"
                    valid=bool(val.match(xmlelem.attrib[ky]))
                if valid:
                    # return whe this filter element is valid else try the next filter
                    return valid
        return False

    def addFilter(self,attr,regex):
        """Appends a filter for a dataset element"""
        if not self._filt:
            self._filt={}
        self._filt[attr]=re.compile(regex)


def gethref(input):
    """small function to extract a href link from a dictionary"""
    for ky,val in input.items():
        if ky.endswith("href"):
            return val
    return None

def searchElem(xml,filter):
    """Search for a specific service element"""
    for elem in xml:
        if filter.isValid(elem):
            return elem
        else:
            #recursively search
            return searchElem(elem,filter)

class OpendapConnector:
    """A class to work with an Opendap server"""
    def __init__(self, filters=None):
        self._filt=[]
        if filters:
           for filt in filters:
                self._filt.append(filt)

    @staticmethod
    def getCatalog(url):
        """Retrieve a catalogue"""
        print("getting Opendap catalog: %s"%(url))
        buf=BytesIO()
        http=httpProvider(url)
        http.downloadFile(buf)
        # print(buf.getvalue())
        return os.path.dirname(url), XMLTree.fromstring(buf.getvalue())


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

    def dataSets(self, xml, baseurl, depth=3):
        """Generator which loops through all the catalogues possibly applying filters
        The depth factor determines the maximum depth in the tree datasets wil be searched for"""

        if depth == 0:
            # signals a stopiteration
            return
        else:
            depth-=1

        for xelem in xml:
            valid=True
            for filt in self._filt:
                if not filt.isValid(xelem):
                    valid=False
                    break

            if filt.isType(xelem):
                continue

            # import pdb;pdb.set_trace()
            # if xelem.tag.endswith('dataset'):
            #     print(depth,xelem.tag,xelem.attrib)
            if xelem.tag.endswith("dataset") and "urlPath" in xelem.attrib:
                yield xelem
            elif xelem.tag.endswith('dataset'):
                yield from self.dataSets(xelem,baseurl,depth)

            if xelem.tag.endswith("catalogRef"):
                #Extract a new sub catalog
                subbaseurl,xmlsub=self.getCatalog(baseurl+"/"+gethref(xelem.attrib))
                yield from self.dataSets(xmlsub, subbaseurl, depth)


