#!/usr/bin/python3
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

#this is intended to be turned into a daemon services at some stage

from geoslurp.PluginManager import PluginManager

import sys
import argparse

def main(argv):
    usage="Download and manage Earth Science data"
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument('-l','--list',action='store_true',help="Show a list of available datasources to download from")
    parser.add_argument('-u','--update',action='store_true',help="update selected datasources")
    parser.add_argument('-r','--remove',action='store_true',help="remove selected datasource files and database entries")
    parser.add_argument('--printconfig',action='store_true',help='Prints out a default configuration file (default file is ~/.geoslurp.yaml)')
    parser.add_argument('--cleancache',action='store_true',help="Clean up the cache directory")
    parser.add_argument('--force',action='store_true',help='enforce action')
    parser.add_argument('--verbose',action='store_true',help='Be more verbose')
    parser.add_argument('datasources',nargs='*',help='Datasources to consider')
    args = parser.parse_args(argv[1:])

    Manager=PluginManager()

    if args.printconfig:
        Manager.printconfig()
        sys.exit(0)
   

    if args.cleancache:
        Manager.cleancache()
    
    if args.list:
        Manager.list()
    
    if args.update:
        if not args.datasources:
            raise Exception("No dataset selected")
        for clname in args.datasources:
            print("Updating "+clname,file=sys.stderr)
            Manager[clname].update(args.force)
    
    
    if args.remove:
        if not args.datasources:
            raise Exception("No dataset selected")
        for clname in args.datasources:
            print("Removing data "+clname,file=sys.stderr)
            Manager[clname].remove()

if __name__ == "__main__":
    main(sys.argv)
