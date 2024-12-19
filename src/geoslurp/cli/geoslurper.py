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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018 -2024

#Command line program to Download and manage Earth science data

import sys
import argparse
from geoslurp.db import Inventory, Credentials
from geoslurp.db import GeoslurpConnector
import json
import logging
from geoslurp.db import Settings
from geoslurp.config.localsettings import readLocalSettings
from geoslurp.config.catalogue import DatasetCatalogue
import getpass
import re

def main():
    argv=sys.argv
    
    # add various arguments to the program
    parser=addCommandLineArgs()

    #make sure the last option is separated and interpreted as a positional argument when it is not an option
    if not argv[-1].startswith('-'):
        argv.insert(-1,"--")
        
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
    geoslurpCatalogue=DatasetCatalogue()

    # Initializes an object which holds the current settings
    conf=Settings(DbConn)
    if args.list:
        if args.dvfexpr is None:
            subsearch=""
        else:
            subsearch=args.dvfexpr

        # show available schemes and datasets
        print("Available datasets (SCHEME.DATASET):")
        for catentry in geoslurpCatalogue.listDataSets(conf):
            if subsearch in catentry:
                print("\t%s"%(catentry))    
        
        print("Available functions (SCHEME.FUNCTION):")
        for catentry in geoslurpCatalogue.listFunctions(conf):
            if subsearch in catentry:
                print("\t%s"%(catentry))
        
        print("Available views (SCHEME.VIEW):")
        for catentry in geoslurpCatalogue.listViews(conf):
            if subsearch in catentry:
                print("\t%s"%(catentry))
        #do not do other stuff after listing
        return 

    #Add a new user
    if args.add_user:
        addUser(DbConn,args.add_user,False)

    if args.add_readonly_user:
        addUser(DbConn,args.add_readonly_user,True)


    #print registered datasets (i.e. tables)
    if args.info:
        slurpInvent=Inventory(DbConn)
        if args.dvfexpr:
            #print a summary of the inventory
            dsetpat=re.compile(args.dvfexpr)
            for entry in slurpInvent:
                if entry.dataset is not None and f"{entry.scheme}.{entry.dataset}" == args.dvfexpr:
                    print("Registered entry:")
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
    
    if args.config:
        #register user settings in the database
        conf.update(args.config)

    if args.show_config:
        conf.show(args.show_config)

    if args.admin_config:
        #register admin/default settings in the database
        conf.defaultupdate(args.admin_config)

    if args.auth_config:
        # import pdb;pdb.set_trace()
        for alias,dvals in args.auth_config.items():
            if dvals == "delete":
                conf.delAuth(alias)
            else:
                conf.updateAuth(Credentials(alias=alias,**dvals))


    if not ( args.dvfexpr or args.func or args.view ):
        #OK jsut gracefully exit
        sys.exit(0)

    if args.dvfexpr and not (args.func or args.view):
        dataset=geoslurpCatalogue.getDsetClass(conf, args.dvfexpr)
    else:
        dataset=None

    if args.func:
        func=geoslurpCatalogue.getDFuncClass(conf, args.dvfexpr)
    else:
        func=None

    if args.view:
        view=geoslurpCatalogue.getViewClass(conf, args.dvfexpr)
    else:
        view=None

    if dataset is None and func is None and view is None:
        print("No valid dataset,function or view selected")
        sys.exit(1)

    # dataset specific help
    if args.help: 
        if args.dvfexpr and not (args.func or args.view):
            print("Detailed info on %s options which may be provided as JSON dictionaries"%(dataset.__name__))
            print("\t%s.pull:\n\t\t %s"%(dataset.__name__,dataset.pull.__doc__))
            print("\t%s.register:\n\t%s"%(dataset.__name__,dataset.register.__doc__))
        if args.func:
            print("Detailed info on %s options which may be provided as JSON dictionaries"%(func.__name__))
            print("\t%s.register:\n\t%s"%(func.__name__,func.register.__doc__))

        if args.view:
            print("Detailed info on %s options which may be provided as JSON dictionaries"%(view.__name__))
            print("\t%s.register:\n\t%s"%(view.__name__,view.register.__doc__))
        sys.exit(0)
    
    
    if not (args.pull or args.register or args.purge_cache or args.purge_data or args.purge_entry or args.export):
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
    
    if dataset is not None:
        #initialize the class
        ds=dataset(DbConn)

        if args.data_dir:
            ds.setDataDir(args.data_dir)
        
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

        if args.export:
            ds.export(args.export)

        #We need to explicitly delete the dataset instance or else the database QueuePool gets exhausted 
        del ds
    
    #loop over requested function
    if func is not None:
        #initialize the class
        df=func(DbConn)

        if args.register:
            df.register(**regopts)
        
        if args.purge_entry:
            df.purgeentry()
        #We need to explicitly delete the function instance or else the database QueuePool gets exhausted 
        del df

    #loop over requested view
    if view is not None:
        #initialize the class
        dv=view(DbConn)

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
                             help="Prints detailed help (may be used in combination with a positional argument matching a certain dataset for detailed JSON options)")
        #Look for a datasets, view or functionsto manage
        parser.add_argument("dvfexpr",metavar="SCHEMA.ITEM",nargs="?",type=str,default=None,
                help='Select a fully qualified dataset, function or view specified as schema.tablename/function/view. use --func  or --view to specify the type considered (default is a dataset')

        parser.add_argument("-f","--func",action="store_true",
                help='Use geoslurp database functions')
        
        parser.add_argument("-V","--view",action="store_true",
                help='Use geoslurp database views)')
        parser.add_argument('-i','--info',action='store_true',
                            help="Show information about selected datasets")

        parser.add_argument('-l','--list',action='store_true',
                            help="List all datasets which are available to use. When a positional argument is supplied it will be used as a search string")


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
        parser.add_argument("--export", metavar="OUTPUTFILE",type=str, nargs="?",const="auto", default=False,
                            help="Export the selected tables in a SQLITE or geopackage file. The type of output is determined from the OUTPUTFILE extension (.sql or .gpkg). When no OUTPUTFILE is provided an SQLITE or gpkg file is dumped in the current directory (depending on whether thee table has a geometry columns.")

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
                                 "to increase verbosity. The default prints errors and info")

        parser.add_argument('--data-dir',type=str,metavar='DIR',
                help="Specify a dataset specific data directory DIR")
        
        # parser.add_argument('--cache-dir',type=str,metavar='DIR',nargs=1,
        #         help="Specify (and register) a dataset specific cache directory DIR")

        parser.add_argument('--cache',type=str,metavar='DIR',
                            help="Set the root of the cache directory to DIR")


        # parser.add_argument("--mirror",metavar="MIRRORALIAS",nargs="?",type=str,
                # help="Use a different mirror for prepending to relative filename uris, the default uses the mirror registered as 'default' in the database. A mirror can be registered in the database  with --[admin-]config '{\"MirrorMaps\":{\"MIRRORALIAS\":\"MIRRORPATH\"}}'.")
        

        return parser

def check_args(args,parser):
    """Sanity check for input arguments and possibly supply detailed help"""

    if not any(vars(args).values()):
        print(__file__+' Error: no arguments provided, try --help', file=sys.stderr)
        sys.exit(1)

    if args.help:
        if not ( args.dvfexpr or args.func ) :
            parser.print_help()
            sys.exit(0)

    #also fillout last options with defaults from the last call
    #note that this will return an updated argument dict
    return readLocalSettings(args,readonlyuser=False)


if __name__ == "__main__":
    main()
