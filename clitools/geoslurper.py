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
from geoslurp.db import Inventory, Credentials
from geoslurp.db import GeoslurpConnector
import json
import logging
from geoslurp.db import Settings
from geoslurp.config.localsettings import readLocalSettings
from geoslurp.config.catalogue import geoslurpCatalogue
import getpass
import re

def main(argv):

    # add various arguments to the program
    parser=addCommandLineArgs()
    args = parser.parse_args(argv[1:])
    args=check_args(args,parser)

    # We need a point of contact to communicate with the database
    try:
        DbConn=GeoslurpConnector(args.host,args.user,args.password,cache=args.cache,dataroot=args.dataroot)
    except Exception as e:
        print(e)
        print("Cannot connect to postgresql database, quitting")
        sys.exit(1)


    # # Process common options



    #Add a new user
    if args.add_user:
        addUser(DbConn,args.add_user,False)

    if args.add_readonly_user:
        addUser(DbConn,args.add_readonly_user,True)



    #print registered datasets (i.e. tables)
    if args.info:
        slurpInvent=Inventory(DbConn)
        print("Registered entries:")
        if args.dset:
            #print a summary of the inventory
            dsetpat=re.compile(args.dset)
            for entry in slurpInvent:
                if entry.dataset is not None and dsetpat.fullmatch(entry.scheme+'.'+entry.dataset):
                    print({ky:val for ky,val in entry.__dict__.items() if ky not in ['_sa_instance_state','view','pgfunc']})
        
        elif args.view:
            #print a summary of the inventory
            viewpat=re.compile(args.view)
            for entry in slurpInvent:
                if entry.view is not None and viewpat.fullmatch(entry.scheme+'.'+entry.view):
                    print({ky:val for ky,val in entry.__dict__.items() if ky not in ['_sa_instance_state','dataset','pgfunc']})
        elif args.func:
            #print a summary of the inventory
            funcpat=re.compile(args.func)
            for entry in slurpInvent:
                if entry.pgfunc is not None and funcpat.fullmatch(entry.scheme+'.'+entry.pgfunc):
                    print({ky:val for ky,val in entry.__dict__.items() if ky not in ['_sa_instance_state','dataset','view']})
        else:
            #print a summary of the inventory (registered datasets, views, functions)
            for entry in slurpInvent:
                if entry.dataset is not None:
                    print("DATASET: %s.%s %s %s"%(entry.scheme,entry.dataset,entry.owner,entry.lastupdate.isoformat()))
                elif entry.view is not None:
                    print("VIEW: %s.%s %s %s"%(entry.scheme,entry.view,entry.owner,entry.lastupdate.isoformat()))
                elif entry.pgfunc is not None:
                    print("FUNCTION: %s.%s %s %s"%(entry.scheme,entry.pgfunc,entry.owner,entry.lastupdate.isoformat()))
        sys.exit(0) 
    
    #change settings in the database
    
    # Initializes an object which holds the current settings
    conf=Settings(DbConn)
    if args.config:
        #register user settings in the database
        conf.update(args.config)

    if args.show_config:
        conf.show(args.show_config)

    if args.admin_config:
        #register admin/default settings in the database
        conf.defaultupdate(args.admin_config)

    if args.auth_config:
        for alias,dvals in args.auth_config.items():
            if dvals == "delete":
                conf.delAuth(alias)
            else:
                conf.updateAuth(Credentials(alias=alias,**dvals))
    
    if args.refresh:
        if args.refresh != "DEFAULTPATH":
            #explicitly supply additional user plugin paths
            geoslurpCatalogue.setUserPlugPaths(args.refresh.split(";"))

        geoslurpCatalogue.refresh(conf)

    if args.list:
        # show available schemes and datasets
        print("Available datasets (SCHEME.DATASET):")
        for catentry in geoslurpCatalogue.listDataSets(conf).keys():
            print("\t%s"%(catentry))
        
        print("Available functions (SCHEME.FUNCTION):")
        for catentry in geoslurpCatalogue.listFunctions(conf).keys():
            print("\t%s"%(catentry))
        
        print("Available views (SCHEME.VIEW):")
        for catentry in geoslurpCatalogue.listViews(conf).keys():
            print("\t%s"%(catentry))

    if not ( args.dset or args.func or args.view ):
        #OK jsut gracefully exit
        sys.exit(0)

    if args.dset:
        datasets=geoslurpCatalogue.getDatasets(conf, args.dset)
    else:
        datasets=[]

    if args.func:
        funcs=geoslurpCatalogue.getFuncs(conf, args.func)
    else:
        funcs=[]
     
    if args.view:
        views=geoslurpCatalogue.getViews(conf, args.view)
    else:
        views=[]

    if not ( datasets or funcs or views ):
        print("No valid datasets or functions selected")
        sys.exit(1)

    # dataset specific help
    if args.help: 
        if args.dset:
            for ds in datasets:
                print("Detailed info on %s options which may be provided as JSON dictionaries"%(ds.__name__))
                print("\t%s.pull:\n\t\t %s"%(ds.__name__,ds.pull.__doc__))
                print("\t%s.register:\n\t%s"%(ds.__name__,ds.register.__doc__))
        if args.func:
            for df in funcs:
                print("Detailed info on %s options which may be provided as JSON dictionaries"%(df.__name__))
                print("\t%s.register:\n\t%s"%(df.__name__,df.register.__doc__))

        if args.view:
            for dv in views:
                print("Detailed info on %s options which may be provided as JSON dictionaries"%(dv.__name__))
                print("\t%s.register:\n\t%s"%(dv.__name__,dv.register.__doc__))
        sys.exit(0)
    
    
    if not (args.pull or args.register or args.purge_cache or args.purge_data or args.purge_entry):
        sys.exit(0)
    
    if args.pull:
        if type(args.pull) == dict:
            pullopts=args.pull
        else:
            pullopts={}
        args.pull=True

    if args.register:
        if type(args.register) == dict:
            regopts=args.register
        else:
            regopts={}
        args.register=True
    
    for dsclass in datasets:
        #initialize the class
        ds=dsclass(DbConn)

        if args.data_dir:
            ds.setDataDir(args.data_dir)
        
        # if args.cache_dir:
        #     ds.setCacheDir(args.cache_dir)

        # import pdb;pdb.set_trace()
        if args.purge_cache:
            ds.purgecache(args.purge_cache)

        if args.purge_data:
            ds.purgedata(args.purge_data)
        # import pdb;pdb.set_trace()
        if args.purge_entry:
            ds.purgeentry()

        if args.pull:
            try:
                ds.pull(**pullopts)
            except KeyboardInterrupt:
                ds.halt()

        if args.register:
            try:
                ds.register(**regopts)
            except KeyboardInterrupt:
                ds.halt()
        #We need to explicitly delete the dataset instance or else the database QueuePool gets exhausted 
        del ds
    
    #loop over requested functions
    for dfclass in funcs:
        #initialize the class
        df=dfclass(DbConn)

        if args.register:
            df.register(**regopts)
        
        if args.purge_entry:
            df.purgeentry()
        #We need to explicitly delete the function instance or else the database QueuePool gets exhausted 
        del df

    #loop over requested views
    for dvclass in views:
        #initialize the class
        dv=dvclass(DbConn)

        if args.register:
            dv.register(**regopts)
        
        if args.purge_entry:
            dv.purgeentry()
        #We need to explicitly delete the function instance or else the database QueuePool gets exhausted 
        del dv


def addUser(conn,user,readonly):
    userpass=user.split(":")
    if len(userpass) == 1:
        userpass.append(getpass.getpass(prompt='Please enter new password: '))
        passwcheck=getpass.getpass(prompt='Reenter password: ')
        if userpass[1] != passwcheck:
            print("Passwords do not match, please try again")
            sys.exit(1)
    else:
        passw1=userpass[1]

    # import ipdb;ipdb.set_trace()
    conn.addUser(userpass[0],userpass[1],readonly)

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


# see https://stackoverflow.com/questions/34735831/python-argparse-toggle-no-toggle-flag
class NegateAction(argparse.Action):
    """Toggle option between True and False depending on whether the option starts with --no"""
    def __call__(self, parser, ns, values, option):
        setattr(ns, self.dest, option[2:4] != 'no')

def addCommandLineArgs():
        """Add top level command line arguments (and request arguments from the loaded schema)"""
        usage=" Program to download and manage Earth Science data"
        parser = argparse.ArgumentParser(description=usage,add_help=False)

        parser.add_argument('-h','--help',action='store_true',
                             help="Prints detailed help (may be used in combination with --dset for detailed JSON options)")
        parser.add_argument('-i','--info',action='store_true',
                            help="Show information about selected datasets")

        parser.add_argument('-l','--list',action='store_true',
                            help="List all datasets which are available to use")

        parser.add_argument("--refresh",metavar="USERPLUGINPATHS",nargs="?",type=str,const="DEFAULTPATH", help="Refresh the cache of the available datasets, views and functions. Optionally supply a set of USERPLUGINPATHS (seperate multiple paths with a semicolon) where dataset implementations can be found.")

        # parser.add_argument('--purge-scheme',action='store_true',
        #                     help="Purge selected scheme (This deletes all related datasets as well!")

        parser.add_argument('--purge-cache',type=str, metavar='filter',const='*',nargs='?',
                            help="Purge the cache of the selected dataset. while optionally applying a filter for the files")

        parser.add_argument('--purge-data',type=str, metavar='filter',const='*',nargs='?',
                            help="Purge the data of the selected dataset. while optionally applying a filter for the files")

        parser.add_argument('--purge-entry',action='store_true',
                            help="Purge the database entry of the selected dataset.")

        parser.add_argument("--pull", metavar="JSON",action=JsonParseAction, nargs="?",const=False,default=False,
                            help="Pull data from online resource (possibly pass on options as a JSON dict")

        parser.add_argument("--register", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register data in the database (possibly pass on options as a JSON dict)")

        # parser.add_argument("--update", metavar="JSON", action=JsonParseAction, nargs="?",const=False,default=False,
                            # help="Implies both --pull and --register, but applies only to the updated data (accepts JSON options)")

        parser.add_argument("--local-settings",metavar="LOCALSETTINGSFILE",type=str, 
                            help='Specify a different file to read the local settings from (default= ${HOME}/.geoslurp_lastused.yaml)')


        parser.add_argument("--write-local-settings",action="store_true",
                            help='Write relevant command line options to local-settings file')

        parser.add_argument("--config", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register user settings  (pass as a JSON dict, e.g. {\"DataDir\":\"path/\"})")


        parser.add_argument("--show-config",type=str,metavar="SHOWPASS", help="Show user configurations as stored in the database, specify '--show-config nohide' to also show passwords",nargs="?",const="hide",default=False)


        parser.add_argument("--auth-config", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register (and encrypt on a user basis in the database) authentification services "
                            "(pass as a JSON dict, e.g. {\"servicename\":{\"user\":..,\"passw\":...}}). To delete an entry specify {\"servicename\":\"delete\"}")
        
        parser.add_argument("--admin-config", metavar="JSON",action=JsonParseAction, nargs="?",const=False, default=False,
                            help="Register admin/default settings  (pass as a JSON dict, e.g. {\"DataDir\":\"path/\"})")
       
        parser.add_argument("--host",metavar="hostname",type=str,nargs="?",default=False,const="unixsocket",
                            help='Select host where the PostgreSQL/PostGIS server is running. Specifying --host without a hostname will try to connect to the local unix socket')

        parser.add_argument("--user",metavar="user",type=str,
                            help='Select postgresql user')

        parser.add_argument("--add-user",metavar="username",type=str,
                help='Add a new postgresql user (you will be prompted for a password, or you can append the password after a colon e.g. pietje:secretpassword)')
        
        parser.add_argument("--add-readonly-user",metavar="username",type=str,
                            help='Add a new readonly postgresql user (you will be prompted for a password, or you can append the password after a colon e.g. pietje:secretpassword)')


        parser.add_argument("--password",metavar="password",type=str,
                            help='Select password for the postgresql user')

        parser.add_argument("--port",metavar="port",type=int, default=5432,
                            help='Select the port where the database is served')

        parser.add_argument("--dataroot",metavar="DATAROOT",nargs="?",type=str, help="Specify the local root of the data directory. Defaults to ${HOME}/geoslurp_data")


        parser.add_argument("--dbalias",metavar="DBALIAS",nargs="?",type=str, help="Specify the database alias to connect to. Each database alias can have a different host,port,user,password,dataroot,etc (see the localsettings file")
        # parser.add_argument("--usekeyring",action='store_true',
        #                     help='Set and get the system keyring to store the database password (alternatives are '
        #                          'using --password or the environment variable GEOSLURP_PGPASS')
        parser.add_argument("--keyring","--no-keyring",dest="usekeyring",action=NegateAction, nargs=0,
                            help=" Use/don't use the system keyring to store and retrieve the database password (alternatives are "
                                 "using --password or the environment variable GEOSLURP_PGPASS")

        parser.add_argument("-v","--verbose", action=increaseVerboseAction, nargs="?",const='',default=3,
                            help="Increase verbosity of the output one cvan use multiple v's after another (e.g. -vv) "
                                 "to increase verbosity. The default prints errors only")

        parser.add_argument('--data-dir',type=str,metavar='DIR',
                help="Specify a dataset specific data directory DIR")
        
        # parser.add_argument('--cache-dir',type=str,metavar='DIR',nargs=1,
        #         help="Specify (and register) a dataset specific cache directory DIR")

        parser.add_argument('--cache',type=str,metavar='DIR',
                            help="Set the root of the cache directory to DIR")

        #also look for datasets or functions  to manage
        parser.add_argument("-d","--dset",metavar="PATTERN",nargs="?",type=str,
                help='Select datasets or all datasets in a scheme (PATTERN is treated as a regular expression applied to the string SCHEME.DATASET)')

        # parser.add_argument("--mirror",metavar="MIRRORALIAS",nargs="?",type=str,
                # help="Use a different mirror for prepending to relative filename uris, the default uses the mirror registered as 'default' in the database. A mirror can be registered in the database  with --[admin-]config '{\"MirrorMaps\":{\"MIRRORALIAS\":\"MIRRORPATH\"}}'.")
        
        parser.add_argument("-f","--func",metavar="PATTERN",nargs="?",type=str,
                help='Select geoslurp database functions or all functions in a scheme (PATTERN is treated as a regular expression applied to the string SCHEME.FUNCTION)')

        parser.add_argument("-V","--view",metavar="PATTERN",nargs="?",type=str,
                help='Select geoslurp database views or all views in a scheme (PATTERN is treated as a regular expression applied to the string SCHEME.VIEW)')
        return parser

def check_args(args,parser):
    """Sanity check for input arguments and possibly supply detailed help"""

    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help', file=sys.stderr)
        sys.exit(1)

    if args.help:
        if not ( args.dset or args.func ) :
            parser.print_help()
            sys.exit(0)

    #also fillout last options with defaults from the last call
    #note that this will return an updated argument dict
    return readLocalSettings(args,readonlyuser=False)


if __name__ == "__main__":
    main(sys.argv)
