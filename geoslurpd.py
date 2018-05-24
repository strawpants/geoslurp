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

#this is intended to be turned into a daemon services at some stage (e.g. handling the options through json posts)

from geoslurp.taskScheduler import taskScheduler

import sys
import argparse
import os.path

def main(argv):
    usage="Download, manage and query Earth Science data"
    parser = argparse.ArgumentParser(description=usage)
    #load settings and start a taskmanager instance
      
    taskManager=taskScheduler(os.path.join(os.path.expanduser('~'),'.geoslurp.yaml'))
    taskManager.addArgs(parser)
    
    
    
    args = parser.parse_args(argv[1:])

    #now process datasources with their parsed arguments
    taskManager.execTasks(args)

if __name__ == "__main__":
    main(sys.argv)
