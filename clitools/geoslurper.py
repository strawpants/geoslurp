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
from geoslurp.db import GeoslurpConnector
from geoslurp.schema import allSchemes, schemeFromName
import json
import logging
from geoslurp.db import Settings
import yaml
from datetime import datetime
import keyring

def main(argv):
    usage=" Program to download and manage Earth Science data"
    parser = argparse.ArgumentParser(description=usage,add_help=False)


    # add various arguments to the program
    addCommandLineArgs(parser)

    args = parser.parse_args(argv[1:])
    check_args(args,parser)




    # We need a point of contact to communicate with the database
    try:

        DbConn=GeoslurpConnector(args.host,args.user,args.password)
        # Initializes an object which holds the current inventory
        slurpInvent=Inventory(DbConn)
        conf=Settings(DbConn,args.user,args.password)

    except Exception as e:
        print("Cannot connect to postgresql database, quitting")
        sys.exit(1)

    # Process common options
    if args.list:
        # show available schemes and datasets
        showAvailable(conf)

    if args.settings:
        #register settings in the database
        conf.update(args.settings)

    if args.auth:
        conf.updateAuth(args.auth)


    if not args.dset and args.info:
        #list the inventory of all the registered schemas but don't list info on the datasets
        for row in slurpInvent:
            print("registered schema:", row.scheme)
        sys.exit(0)

    if not args.dset:
        # Nothing to do anymore so exit
        sys.exit(0)
    else:
        # Initialize scheme
        scheme=schemeFromName(args.dset["scheme"])(slurpInvent, conf)
        # And the requested Datasets
        scheme.initDataSets(args.dset["datasets"])

    if args.purge_scheme:
        scheme.purge()
        # Exit as there are no datasets to work on anymore
        sys.exit(0)


    if args.info:
    #info on selected data sources
        for ds in scheme:
            print("Schema and dataset: %s.%s"%(scheme._schema,ds.name))
            dsentry=ds.info()
            for ky,val in dsentry.items():
                print("\t\t%s = "%(ky),end="")
                print(val)

    if args.purge:
        for ds in scheme:
            ds.purge()

    if args.pull or args.update:
        if type(args.pull) == dict:
            opts=args.pull
        elif type(args.update) == dict:
            opts=args.update
        else:
            opts={}

        for ds in scheme:
            try:
                ds.pull(**opts)
            except KeyboardInterrupt:
                ds.halt()


    if args.register or args.update:
        if type(args.register) == dict:
            opts=args.register
        elif type(args.update) == dict:
            opts=args.update
        else:
            opts={}

        try:
            for ds in scheme:
                ds.register(**opts)
        except KeyboardInterrupt:
            #try shutting down gracefully
            for ds in scheme:
                try:
                    ds.halt()
                except Exception as e:
                    #OK move on to the next dataset
                    pass
            sys.exit(1)



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

class JsonParseAction(argparse.Action):
    """Parse Arguments provided as JSON into dictionaries"""
    def __init__(self, option_strings, dest, nargs, **kwargs):
        super(JsonParseAction, self).__init__(option_strings, dest,nargs, **kwargs)
    def __call__(self, parser=None, namespace=None, values=None, option_string=None):
        if values:
            dct=json.loads(values)
        else:
            dct=True
        setattr(namespace, self.dest, dct)

class increaseVerboseAction(argparse.Action):
    """Parse multiple v's to increase the level of the verbosity"""
    def __init__(self, option_strings, dest, nargs, **kwargs):
        super(increaseVerboseAction, self).__init__(option_strings, dest,nargs, **kwargs)
    def __call__(self, parser=None, namespace=None, values=None, option_string=None):
        setattr(namespace,self.dest,namespace.verbose+values.count('v'))
        levels=[logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO,logging.DEBUG]
        logging.basicConfig(level=levels[min(namespace.verbose,4)])

def addCommandLineArgs(parser):
        """Add top level command line arguments (and request arguments from the loaded schema)"""
        parser.add_argument('-h','--help',action='store_true',
                             help="Prints detailed help (may be used in combination with --dset for detailed JSON options)")
        parser.add_argument('-i','--info',action='store_true',
                            help="Show information about registered schemes and datasets")

        parser.add_argument('-l','--list',action='store_true',
                            help="List schemes and datasets which are available to use")

        parser.add_argument('--purge',action='store_true',
                            help="Purge selected datasets")

        parser.add_argument('--purge-scheme',action='store_true',
                            help="Purge selected scheme (This deletes all related datasets as well!")

        parser.add_argument("--pull", metavar="JSON",action=JsonParseAction, nargs="?",const=False,default=False,
                            help="Pull data from online resource (possibly pass on options as a JSON dict")

        parser.add_argument("--register", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register data in the database (possibly pass on options as a JSON dict)")

        parser.add_argument("--update", metavar="JSON", action=JsonParseAction, nargs="?",const=False,default=False,
                            help="Implies both --pull and --register, but applies only to the updated data (accepts JSON options)")


        parser.add_argument("--settings", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register settings  (pass as a JSON dict, e.g. {\"DataDir\":\"path/\"})")


        parser.add_argument("--auth", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register (and encrypt in the database) authentification services "
                                 "(pass as a JSON dict, e.g. {\"servicename\":{\"user\":..,\"passw\":...}})")
        # parser.add_argument('--printconfig',action='store_true',help='Prints out default configuration (default file is ~/.geoslurp.yaml)')
        # parser.add_argument('--cleancache',action='store_true',
        #                     help="Clean up the cache directory associated with a scheme/dataset")

        parser.add_argument("--host",metavar="hostname",type=str,
                            help='Select host where the PostgreSQL/PostGIS server is running')

        parser.add_argument("--user",metavar="user",type=str,
                            help='Select postgresql user')


        parser.add_argument("--password",metavar="password",type=str,
                            help='Select password for the postgresql user')

        parser.add_argument("--usekeyring",action='store_true',
                            help='Set and get the system keyring to store the database password (alternatives are '
                                 'using --password or the environment variable GEOSLURP_PGPASS')

        parser.add_argument("-v","--verbose", action=increaseVerboseAction, nargs="?",const='',default=3,
                            help="Increase verbosity of the output one cvan use multiple v's after another (e.g. -vv) "
                                 "to increase verbosity. The default prints errors only")
        #also add datasource options
        parser.add_argument("-d","--dset",metavar="SCHEME[.DATASET]",nargs="?",action=SplitAction,
                            help='Scheme and dataset to select.')


def showAvailable(conf):
    """Print available schemes with implemented datasets"""
    print("Allowed scheme and dataset combinations:")
    for key,cl in allSchemes().items():
        print("\t %s"%(key))
        for dname in cl.listDsets(conf):
            print("\t\t.%s"%(dname))


def check_args(args,parser):
    """Sanity check for input arguments and possibly supply detailed help"""

    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help', file=sys.stderr)
        sys.exit(1)

    if args.help:
        if not args.dset:
            parser.print_help()
        else:
            print("Detailed info on options which may be provided as JSON dictionaries")
            # print("\n\n %s"%(schemeFromName(args.input["scheme"]).__datasets__[args.input["dataset"]].__doc__))
            for dSets in args.dset["datasets"]:
                print("\t%s.pull:\n\t\t %s"%(dSets,schemeFromName(args.dset["scheme"]).__datasets__[dSets].pull.__doc__))
                print("\t%s.register:\n\t%s"%(dSets,schemeFromName(args.dset["scheme"]).__datasets__[dSets].register.__doc__))

        sys.exit(0)
    #also fillout last options with defaults from the last call
    getUpdateLastOptions(args)

def getUpdateLastOptions(args):
    """Retireves last """
    settingsFile=os.path.join(os.path.expanduser('~'),'.geoslurp_last.yaml')
    #read last used settings
    if os.path.exists(settingsFile):
        #Read parameters from yaml file
        with open(settingsFile, 'r') as fid:
            lastOpts=yaml.safe_load(fid)
    else:
            lastOpts={}

    isUpdated=False

    #update dict with provided options from args
    if args.host:
        lastOpts["dbhost"]=args.host
        isUpdated=True
    else:
        #update options
        try:
            args.host=lastOpts["dbhost"]
        except KeyError:
            print("--host option is needed for initialization",file=sys.stderr)
            sys.exit(1)

    if args.user:
        lastOpts["dbuser"]=args.user
        isUpdated=True
    else:
        try:
            args.user=lastOpts["dbuser"]
        except KeyError:
            print("--user option is needed for initialization",file=sys.stderr)
            sys.exit(1)

    if args.usekeyring:
        lastOpts["useKeyring"]=True
        isUpdated=True
    else:
        try:
            args.usekeyring=lastOpts["useKeyring"]
        except KeyError:
            #don't use the keyring
            pass

    #write out  options to file to store these settings
    if isUpdated:
        lastOpts["lastupdate"]=datetime.now()
        with open(settingsFile,'w') as fid:
            yaml.dump(lastOpts, fid, default_flow_style=False)


    # we take a different strategy for the password as we don't want to store this in a file
    if not args.password:
        if args.usekeyring:
            args.password=keyring.get_password("geoslurp","dbpassword")
            if not args.password:
                print("could not retrieve password from keyring, use --password PASSWORD to store it, quitting",file=sys.stderr)
                sys.exit(1)
        else:
        #try checking the environment variable GEOSLURP_PGPASS
            try:
                args.password=os.environ["GEOSLURP_PGPASS"]
            except KeyError:
                print("Database password is not set, quitting",file=sys.stderr)
                sys.exit(1)
    else:
        #update keyring
        if args.usekeyring:
            keyring.set_password("geoslurp","dbpassword",args.password)


if __name__ == "__main__":
    main(sys.argv)
