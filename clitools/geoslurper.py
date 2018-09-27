#!/usr/bin/env python3
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

#Command line program to Download and manage Earth science data

import sys
import argparse
import os.path
from geoslurp.db import SchemaManager
from geoslurp.config import SlurpConf

def main(argv):
    usage=" Program to download and manage Earth Science data"
    parser = argparse.ArgumentParser(description=usage)


    #initializes an object which scans the available schemas/datasources
    Schemas=SchemaManager()

    conf=SlurpConf(os.path.join(os.path.expanduser('~'), '.geoslurp.yaml'))


    #add various arguments to the program
    addCommandLineArgs(parser,Schemas)

    
    
    args = parser.parse_args(argv[1:])
    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help',file=sys.stderr)
        sys.exit(1)


    #now process datasources with their parsed arguments
    execTasks(Schemas,args)

def addCommandLineArgs(parser,SchemaCtrl):
        """Add top level command line arguments (and request arguments from the loaded schema)"""

        parser.add_argument('-l','--list',action='store_true',help="list registered schema/datasources from the inventory")
        parser.add_argument('--purge',action='store_true',help="Purge selected datasource(=schema) from the database (all associate datasets and data files)")
        # parser.add_argument('--printconfig',action='store_true',help='Prints out default configuration (default file is ~/.geoslurp.yaml)')
        # parser.add_argument('--cleancache',action='store_true',help="Clean up the cache directory (optionally refine by specifing a DATASOURCE)")
        #also add datasource options
        subparsers = parser.add_subparsers(help='Datasource to select (type geoslurper DATASOURCE --help for detailed options)',dest='datasource')
        for key,cl in SchemaCtrl.items():
            subparser = subparsers.add_parser(key, help=cl.__doc__)


def executeTasks(schemas,args):
    print("Executing schemas tasks")

if __name__ == "__main__":
    main(sys.argv)
