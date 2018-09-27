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
from geoslurp.db import Inventory
from geoslurp.config import SlurpConf
from geoslurp.db import GeoslurpConnector
from geoslurp.schema import availableSchemes, schemeFromName

def main(argv):
    usage=" Program to download and manage Earth Science data"
    parser = argparse.ArgumentParser(description=usage)


    conf=SlurpConf(os.path.join(os.path.expanduser('~'), '.geoslurp.yaml'))



    #add various arguments to the program
    addCommandLineArgs(parser)

    args = parser.parse_args(argv[1:])
    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help', file=sys.stderr)
        sys.exit(1)


    #We need a point of contact to communicate with the database
    DbConn=GeoslurpConnector(conf['dburl'])
    #initializes an object which scans the available schemas/datasources
    Schemas=Inventory(DbConn)

    #process common options
    if args.list:
        #print a list of registered datasources
        for row in Schemas.items():
            print("Datasource/Schema:", row.datasource, ", last updated:", row.lastupdate)

    if args.datasource:
        scheme=schemeFromName(args.datasource)(Schemas, conf)
    else:
        #nothing to do anymore
        sys.exit(0)

    if args.purge:
        scheme.purge()

    if args.Datasets:
        print(args.Datasets)


def addCommandLineArgs(parser):
        """Add top level command line arguments (and request arguments from the loaded schema)"""

        parser.add_argument('-l','--list',action='store_true',help="list registered the inventory (registered schema/datasources)")
        parser.add_argument('--purge',action='store_true',help="Purge selected datasource(=schema) from the database (all associate datasets and data files)")
        # parser.add_argument('--printconfig',action='store_true',help='Prints out default configuration (default file is ~/.geoslurp.yaml)')
        # parser.add_argument('--cleancache',action='store_true',help="Clean up the cache directory (optionally refine by specifing a DATASOURCE)")
        #also add datasource options
        subparsers = parser.add_subparsers(help='Datasource to select (type geoslurper DATASOURCE --help for detailed options)',dest='datasource')
        for key, cl in availableSchemes().items():
            subparser = subparsers.add_parser(key, help=cl.__doc__)
            subparser.add_argument(help="Select datasets to apply the options to", nargs="*", dest='Datasets')

def executeTasks(schemas,args):
    print("Executing schemas tasks")

if __name__ == "__main__":
    main(sys.argv)
