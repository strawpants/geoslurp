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
from geoslurp.config import slurplog


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
    dataroot=None

    local_settings=None
    """(str): Alternative local settings file (instead of ${HOME}/.geoslurp_lastused.yaml)"""

    cache=None
    write_local_settings=False
    dbalias=None
    def __init__(self,host=None,user=None,usekeyring=False,password=None,port=None,dataroot=None,cache=None,dbalias=None):
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
        
        if dataroot:
            self.dataroot=dataroot
        if cache:
            self.cache=cache

        if dbalias:
            self.dbalias=dbalias
        

defaultdbdict={"host":None,"user":"geoslurp","port":5432,"readonlyUser":"slurpy",
        "cache":"/tmp/geoslurp_cache","dataroot":os.path.join(os.path.expanduser('~'),'geoslurp_data')}
        


def readLocalSettings(args=settingsArgs(),readonlyuser=True,dbalias=None):
    """Retrieves/updates last used settings from the local settings file .geoslurp_lastused.yaml"""

    #We need a deepcopy to properly separate input from output, and funny behavior
    argsout=copy.deepcopy(args)
    
    if dbalias:
        #overrules possible value from args
        argsout.dbalias=dbalias

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
        if not argsout.dbalias:
            argsout.dbalias="geoslurp"
        #set the defaults (note a host with None will try to connect to a local unix socket
        lastOpts={"dbalias":args.dbalias,args.dbalias:defaultdbdict}

    isUpdated=False

    if argsout.dbalias:
        lastOpts["dbalias"]=argsout.dbalias
        dbalias=argsout.dbalias
    else:
        #ue the default from the configuration file
        dbalias=lastOpts["dbalias"]
        argsout.dbalias=dbalias

    if not dbalias in lastOpts:
        #create a defaukt entry for this alias
        lastOpts[dbalias]=defaultdbdict

    #update dict with provided options from argsout
    if argsout.host:
        lastOpts[dbalias]["host"]=argsout.host
        isUpdated=True
    else:
        #take data from file options
        try:
            argsout.host=lastOpts[dbalias]["host"]
        except KeyError:
            #this will be interpreted as a unix socket
            argsout.host=""
            # print("--host option is needed for initialization",file=sys.stderr)
            # sys.exit(1)

    if argsout.port:
        lastOpts[dbalias]["port"]=argsout.port
        isUpdated=True
    else:
        lastOpts[dbalias]["port"]=5432

    if argsout.user:
        if readonlyuser:
            lastOpts[dbalias]["readonlyUser"]=argsout.user
        else:
            lastOpts[dbalias]["user"]=argsout.user
        isUpdated=True
    else:
        try:
            if readonlyuser:
                argsout.user=lastOpts[dbalias]["readonlyUser"]
            else:
                argsout.user=lastOpts[dbalias]["user"]

        except KeyError:
            print("--user option is needed for initialization",file=sys.stderr)
            sys.exit(1)

    if argsout.usekeyring != None:
        #use the provided value (when explicitly set to False or True)
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
            try:
                argsout.password=keyring.get_password("geoslurp",argsout.user)
                hasBackend=True
            except RuntimeError:
                slurplog.warning("no suitable python keyring backend found")
                argsout.password=None
                hasBackend=False

            if not argsout.password:
                argsout.password=getpass.getpass(prompt='Please enter password for %s: '%(argsout.user))
                if hasBackend:
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
                if "passwd" in lastOpts[dbalias] and not readonlyuser:
                    argsout.password=lastOpts[dbalias]["passwd"]
                elif "readonlyPasswd" in lastOpts[dbalias] and readonlyuser:
                    argsout.password=lastOpts[dbalias]["readonlyPasswd"]
                else:
                    #prompt for password
                    argsout.password=getpass.getpass(prompt='Please enter password for %s: '%(argsout.user))
                    if argsout.write_local_settings:
                        print("Warning: user database password will be stored unencrypted in the geoslurp configuration file,"
                              "consider using a keyring")
                        if readonlyuser:
                            lastOpts[dbalias]["readonlyPasswd"]=argsout.password
                        else:
                            lastOpts[dbalias]["passwd"]=argsout.password
    else:
        #update keyring
        if argsout.usekeyring and argsout.write_local_settings:
            keyring.set_password("geoslurp",argsout.user,argsout.password)
    
    if argsout.dataroot:
        #register path to local data root
        lastOpts[dbalias]["dataroot"]=argsout.dataroot
    else:
        if "dataroot" in lastOpts[dbalias]:
            argsout.dataroot=lastOpts[dbalias]["dataroot"]
        else:
            argsout.dataroot=os.path.join(os.path.expanduser('~'),'geoslurp_data')
    
    if argsout.cache:
        lastOpts[dbalias]["cache"]=argsout.cache
    else:
        if "cache"  in lastOpts[dbalias]:
            argsout.cache=lastOpts[dbalias]["cache"]
        else:
            argsout.cache="/tmp/geoslurp_cache"
    

    #write out  options to file to store these settings
    if isUpdated and argsout.write_local_settings:
        lastOpts["lastupdate"]=datetime.now()
        with open(settingsFile,'w') as fid:
            yaml.dump(lastOpts, fid, default_flow_style=False)


    return argsout
