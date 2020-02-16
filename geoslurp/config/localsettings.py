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
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018
import os
import sys
import yaml
from datetime import datetime
import keyring
import getpass
import copy


class settingsArgs:
    """Stand-in class with several settings.
    This class can be used as a stand-in for the
    command line argparse arguments from the :ref:`clihelp` """

    host=None
    """(str): Database host name"""
    user=None
    usekeyring=None
    password=None
    port=5432
    """(int): database port to connect to"""
    mirror=None

    local_settings=None
    cache=None
    """(str): Alternative local settings file (instead of ${HOME}/.geoslurp_lastused.yaml)"""

    def __init__(self,host=None,user=None,usekeyring=True,password=None,port=None,mirror=None,cache=None):
        if host:
            self.host=host

        if user:
            self.user=user

        if usekeyring:
            self.usekeyring=usekeyring

        if password:
            self.password=password

        if port:
            self.port=port
        
        if mirror:
            self.mirror=mirror

        if cache:
            self.cache=cache


def readLocalSettings(args=settingsArgs(),update=True,readonlyuser=True):
    """Retrieves/updates last used settings from the local settings file .geoslurp_lastused.yaml"""
    argsout=copy.deepcopy(args)
    if argsout.local_settings:
        settingsFile=argsout.local_settings
    else:
        settingsFile=os.path.join(os.path.expanduser('~'),'.geoslurp_lastused.yaml')
    #read last used settings
    if os.path.exists(settingsFile):
        #Read parameters from yaml file
        with open(settingsFile, 'r') as fid:
            lastOpts=yaml.safe_load(fid)
    else:
        #set the defaults
        lastOpts={"host":"localhost","user":"geoslurp","port":5432,"readOnlyUser":"slurpy","useKeyring":False,"cache":"tmp/geoslurp_cache"}

    isUpdated=False

    #update dict with provided options from argsout
    if argsout.host:
        lastOpts["host"]=argsout.host
        isUpdated=True
    else:
        #take data from file options
        try:
            argsout.host=lastOpts["host"]
        except KeyError:
            #this will be interpreted as a unix socket
            argsout.host=""
            # print("--host option is needed for initialization",file=sys.stderr)
            # sys.exit(1)

    if argsout.port:
        lastOpts["port"]=argsout.port
        isUpdated=True
    else:
        lastOpts["port"]=5432

    if argsout.user:
        if readonlyuser:
            lastOpts["readonlyUser"]=argsout.user
        else:
            lastOpts["user"]=argsout.user
        isUpdated=True
    else:
        try:
            if readonlyuser:
                argsout.user=lastOpts["readonlyUser"]
            else:
                argsout.user=lastOpts["user"]

        except KeyError:
            print("--user option is needed for initialization",file=sys.stderr)
            sys.exit(1)

    if argsout.usekeyring:
        lastOpts["useKeyring"]=True
        isUpdated=True
    else:
        try:
            argsout.usekeyring=lastOpts["useKeyring"]
        except KeyError:
            #don't use the keyring
            pass



    # we take a different strategy for the password as we don't want to store this unencrypted in a file
    if not argsout.password:
        if argsout.usekeyring:
            argsout.password=keyring.get_password("geoslurp",argsout.user)
            if not argsout.password:
                argsout.password=getpass.getpass(prompt='Please enter password for %s: '%(argsout.user))
                keyring.set_password("geoslurp",argsout.user,argsout.password)
        else:
            #try checking the environment variable GEOSLURP_PGPASS
            try:
                if readonlyuser:
                    argsout.password=os.environ["GEOSLURP_PGPASSRO"]
                else:
                    argsout.password=os.environ["GEOSLURP_PGPASS"]
            except KeyError:
                #check for password in the lastused file (needs to be manually entered)
                if "passwd" in lastOpts and not readonlyuser:
                    argsout.password=lastOpts["passwd"]
                elif "readonlyPasswd" in lastOpts and readonlyuser:
                    argsout.password=lastOpts["readonlyPasswd"]
                else:
                    #prompt for password
                    argsout.password=getpass.getpass(prompt='Please enter password: ')
                    if update:
                        print("Warning: user database password will be stored unencrypted in the geoslurp configuration file,"
                              "consider setting usekeyring=True")
                        if readonlyuser:
                            lastOpts["readonlyPasswd"]=argsout.password
                        else:
                            lastOpts["passwd"]=argsout.password
    else:
        #update keyring
        if argsout.usekeyring and update:
            keyring.set_password("geoslurp",argsout.user,argsout.password)
    
    if argsout.mirror:
        #register alias to which datamirror to use
        lastOpts["mirror"]=argsout.mirror
    else:
        if "mirror" in lastOpts:
            argsout.mirror=lastOpts["mirror"]
        else:
            argsout.mirror="default"

    if argsout.cache:
        lastOpts["cache"]=argsout.cache
    else:
        if "cache"  in lastOpts:
            argsout.cache=lastOpts["cache"]
        else:
            argsout.cache="/tmp/geoslurp_cache"

    #write out  options to file to store these settings
    if isUpdated and update:
        lastOpts["lastupdate"]=datetime.now()
        with open(settingsFile,'w') as fid:
            yaml.dump(lastOpts, fid, default_flow_style=False)


    return argsout
