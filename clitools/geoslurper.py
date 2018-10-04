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
from geoslurp.schema import allSchemes, schemeFromName

def main(argv):
    usage=" Program to download and manage Earth Science data"
    parser = argparse.ArgumentParser(description=usage,add_help=False)

    conf=SlurpConf(os.path.join(os.path.expanduser('~'), '.geoslurp.yaml'))

    # add various arguments to the program
    addCommandLineArgs(parser )

    args = parser.parse_args(argv[1:])
    check_args(args,parser)



    # Process common options
    if args.list:
       # show available schemes and datasets
        showAvailable()

    # We need a point of contact to communicate with the database
    DbConn=GeoslurpConnector(conf['dburl'])

    # Initializes an object which holds the current inventory
    slurpInvent=Inventory(DbConn)

    if not args.input and args.info:
        #list the inventory of all the registered schemas but don't list info on the datasets
        for row in slurpInvent:
            print("schema:", row.datasource, ", last updated:", row.lastupdate)
        sys.exit(0)

    if not args.input:
        # Nothing to do anymore so exit
        sys.exit(0)
    else:
        # Initialize scheme
        scheme=schemeFromName(args.input["scheme"])(slurpInvent, conf)
        # And the requested Datasets
        scheme.initDataSets(args.input["datasets"])

    if args.purge_scheme:
        scheme.purge()
        # Exit as there are no datasets to work on anymore
        sys.exit(0)


    if args.info:
    #info on selected data sources
        for ds in scheme:
            print("Scheme and dataset: %s.%s"%(args.input["scheme"],ds.name))
            dsentry=ds.info()
            for ky,val in dsentry:
                print("\t\t%s = "%(ky),end="")
                print(val)

    if args.purge:
        for ds in scheme:
            ds.purge()

    if args.pull or args.update:
        for ds in scheme:
            ds.pull()


    if args.register or args.update:
        for ds in scheme:
            ds.register()




class SplitAction(argparse.Action):
    """Process input schemes and Split arguments on a dot (this class appears to be somewhat of an overkill currently but is needed by argparse"""
    def __init__(self, option_strings, dest, nargs, **kwargs):
        super(SplitAction, self).__init__(option_strings, dest, **kwargs)
    def __call__(self, parser=None, namespace=None, values=None, option_string=None):
            tmpl=values.split(".")
            val={"scheme":tmpl[0],"datasets":None}
            if len(tmpl) > 1:
                val["datasets"]=[tmpl[1]]
            setattr(namespace, self.dest, val)

def addCommandLineArgs(parser):
        """Add top level command line arguments (and request arguments from the loaded schema)"""
        parser.add_argument('-h','--help',action='store_true',
                             help="Prints detailed help (my be used in combination with a SCHEME)")
        parser.add_argument('-i','--info',action='store_true',
                            help="Show information about registered schemes and datasets")

        parser.add_argument('-l','--list',action='store_true',
                            help="List schemes and datasets which are available to use")

        parser.add_argument('--purge',action='store_true',
                            help="Purge selected datasets")

        parser.add_argument('--purge-scheme',action='store_true',
                            help="Purge selected scheme (This deletes all related datasets as well!")

        parser.add_argument("--pull", action="store_true",
                            help="Pull data from online resource")

        parser.add_argument("--register", action="store_true",
                            help="Register data in the database")

        parser.add_argument("--opts", type=str, default="",
                            help="Pass a string with additional options to pull/register/update. See dataset specific help for details")

        parser.add_argument("--update", action="store_true",
                            help="Implies both --pull and --register, but applies only to the updated data")
        # parser.add_argument('--printconfig',action='store_true',help='Prints out default configuration (default file is ~/.geoslurp.yaml)')
        parser.add_argument('--cleancache',action='store_true',
                            help="Clean up the cache directory associated with a dataset")
        #also add datasource options
        parser.add_argument(metavar="SCHEME[.DATASET]",nargs="?",dest='input',action=SplitAction,
                            help='Scheme and dataset to select. Not specifying a dataset implies all available')


def showAvailable():
    """Print available schemes with implemented datasets"""
    print("Allowed scheme and dataset combinations:")
    for key,cl in allSchemes().items():
        print("\t %s.["%(key),end="")
        sep=""
        for dname in cl.__datasets__:
            print("%s%s"%(sep,dname),end="")
            sep="|"
        print("]")

def check_args(args,parser):
    """Sanity check for input arguments and possibly supply detailed help"""

    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help', file=sys.stderr)
        sys.exit(1)

    if args.help:
        parser.print_help()
        if args.input:
            print("\n\nDetailed --opts string which may be provided to %s:"%(args.input["scheme"]))


        sys.exit(0)



if __name__ == "__main__":
    main(sys.argv)
