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
from geoslurp.datapull.http import Uri as http
import re
import os
from geoslurp.config.slurplogger import slurplogger
from collections import namedtuple
from dateutil.parser import parse as isoParser

from geoslurp.datapull import UriBase,CrawlerBase


class Uri(UriBase):
    """Thredds URI class"""
    suburl=None
    opendap=None
    def __init__(self,dataxml,services,auth=None):
        self.suburl=dataxml.attrib['urlPath']

        self.opendap=services.baseurl+services.opendap+self.suburl
        #set the main url to the http version
        super().__init__(services.baseurl+services.http+self.suburl,lastmod=getDate(dataxml),auth=auth)




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
            return isoParser(elem.text).replace(tzinfo=None)

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

class Crawler(CrawlerBase):
    """A class to work with an Opendap server"""
    def __init__(self, catalogurl, filter=ThreddsFilter("dataset", attr="urlPath"), followfilter=ThreddsFilter("catalogRef").OR("dataset"),auth=None):
        super().__init__(url=catalogurl)
        #load the root catalog
        self._catalogurl=catalogurl
        self._rootxml=self.getCatalog(catalogurl)
        self.services=self.getServices(self._rootxml,self._catalogurl)
        self._filt=filter
        self._followFilt=followfilter
        self.resuming=False
        self.auth=auth
    def setResumePoint(self,filter,followfilt=None):
        """Sets the filters after which the normal filters will be applied."""
        # stores a copy of the normal filter and set the resume filter to the normal filter
        self._filtcopy=self._filt
        self._filt=filter

        if followfilt:
            self._followFiltcopy=self._followFilt
            self._followFilt=followfilt
        self.resuming=True

    def unsetResumePoint(self):
        """Unset resume point"""
        if not self.resuming:
            return

        self._filt=self._filtcopy
        self._filtcopy=None

        if self._followFiltcopy:
            self._followFilt=self._followFiltcopy
            self._followFiltcopy=None
        self.resuming=False

    @staticmethod
    def getCatalog(url,auth=None):
        """Retrieve a catalogue"""
        slurplogger().info("getting Thredds catalog: %s"%(url))
        buf=http(url,auth=auth).buffer()
        return XMLTree.fromstring(buf.getvalue())


    @staticmethod
    def getServices(catalog,rooturl,depth=2):
        """Retrieves the root for serving files over http url from a catalogue"""
        compoundfilt=ThreddsFilter("service", attr="serviceType", regex="Compound")
        opendapfilt=ThreddsFilter("service", attr="serviceType", regex="(OpenDAP)|(OPENDAP)|(DODS)|(OPeNDAP)")
        httpfilt=ThreddsFilter("service", attr="serviceType", regex="HTTPServer")
        # possibly add other serices if deemed usefull
        servtuple=namedtuple("Service","baseurl opendap catalog http")

        proto,url=rooturl.split('://')
        #also strip off trailing directory to retrieve the root
        baseurl, tmp=url.split('/', 1)
        baseurl=proto+"://"+baseurl
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
                    return Crawler.getServices(elem,rooturl, depth)

        catalogbase=rooturl.strip('catalog.xml')[len(baseurl):]
        return servtuple(baseurl=baseurl,opendap=opendap,catalog=catalogbase,http=http)


    def xmlitems(self, xmlcatalog=None, url=None, depth=10):
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

                #special check whether we're considering a resume here
                if self.resuming:
                    self.unsetResumePoint()
                    # we're going to continue with the element after this one
                    continue

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

                    # We treat CatalogRefs in a special way by retrieving the subcatalog from the thredds server
                    suburl=os.path.dirname(url)+"/"+gethref(xelem)
                    try:
                        subxml=self.getCatalog(suburl)
                    except:
                        # Just ignore this catalog entry upon exceptions
                        slurplogger().warning("Ignoring failed CatalogRef %s"%(suburl))
                        continue
                else:
                    # Otherwise we're just going to look in the children of the current element
                    suburl=url
                    subxml=xelem

                yield from self.xmlitems(subxml, suburl, depth)

    def uris(self,depth=10):
        """Generates a list of threddsURI's (makes use of xmlitems())"""
        # urlFilt=ThreddsFilter("dataset", attr="urlPath")
        for xelem in self.xmlitems(depth=depth):
            if self._filt.isValid(xelem):
                yield Uri(xelem,self.services)


