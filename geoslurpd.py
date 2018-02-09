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
from geoslurp.slurpconf import slurpconf

import sys
import argparse

def main(argv):
    usage="Download and manage datasets"
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument('-l','--list',action='store_true',help="Show a list of available data plugins")
    parser.add_argument('-u','--update',action='store_true',help="update selected datasets")
    parser.add_argument('--default',action='store_true',help='Prints out a default configuration file (default file is ~/.geoslurp.yaml)')
    parser.add_argument('datasets',nargs='*',help='Datasets to consider')
    args = parser.parse_args(argv[1:])

    if args.default:
        conf=slurpconf()
        conf.default(sys.stdout)
        sys.exit(0)
    
    Manager=PluginManager()
    if args.list:
        Manager.list()


if __name__ == "__main__":
    main(sys.argv)
