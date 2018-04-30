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


class Slurper():
    """ (Abstract) Base class to which implemented dataslurper should comply to"""
    def __init__(self,conf):
        """slurper Dummy template for geoslurper plugins""" 
    def update(self):
        """Checks for new data and download accordingly"""
    def clear(self):  
        """ Clears the data cache and deletes the corresponding database table """
    def query(self):
        """ perform and retrieve a datasource specific query """
    def datahook(self):
        """ Converts the downloaded files to alternative formats or enters the data in the database"""
    def addParserArgs(self,subparsers):
        """adds datasource specific help options"""
        parser_a = subparsers.add_parser(type(self).__name__, help=type(self).__doc__)
PlugName=Slurper
