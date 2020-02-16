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

# reads and writes contains a class to work with  the geoslurp inventory table
from sqlalchemy import Column,Integer,String,Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, BYTEA
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy import MetaData

import json
import os
from collections import namedtuple
from geoslurp.config.slurplogger import slurplogger
import sys
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend



class MirrorMap:
    def __init__(self,from_mirror,to_mirror):
        if not (from_mirror.endswith("/") and to_mirror.endswith("/")):
            raise RuntimeError("The url of the data mirrors used for mapping should end with a '/'") 
        self.from_mirror=from_mirror
        self.to_mirror=to_mirror
    def apply(self,url):
        return url.replace(self.from_mirror,self.to_mirror)
    
    def reverseApply(self,url):
        return url.replace(self.to_mirror,self.from_mirror)
    
    def strip(self,url):
        return url.replace(self.from_mirror,"")

def getCreateDir(returndir,mirrormapper=None):
    """creates a directory when not existent and return it
    When a mirrormapper is specified the base is converted"""

    #possibly expand environment variables in the string
    returnex=os.path.expandvars(returndir)

    if mirrormapper:
        returnex=mirrormapper.apply(returnex)

    if not os.path.exists(returnex):
        os.makedirs(returnex)
    return returnex

def stripPasswords(d):
    for serv,dsub in d.items():
        # print(serv)
        for ky,v in dsub.items():
            if ky == "passw" or ky == "oauth":
                d[serv][ky]="*****"
    return d

Credentials=namedtuple("Credentials","user passw alias oauthtoken url")
"""A named tuple to store authentication credentials

Attributes:
    user (str): Username for the service
    alias (str): (obligatory) The short name of this service
    passw (str): The password associated with the username
    oauthtoken (str): An oauth2 token
    url (str): The root url which is linked to this service
"""

Credentials.__new__.__defaults__ = (None,) * len(Credentials._fields)

GSBase=declarative_base(metadata=MetaData(schema='admin'))


class SettingsTable(GSBase):
    """Defines the GEOSLURP POSTGRESQL settings table"""
    __tablename__='settings'
    id=Column(Integer, primary_key=True)
    user=Column(String, unique=True)
    conf=Column(MutableDict.as_mutable(JSONB))
    auth=Column(BYTEA) # stored as blowfish encrypted bytearray


class Settings():
    """Read and write default and user specific settings to and from the database"""
    table=SettingsTable
    mirrorMap=None
    def __init__(self,dbconn):
        self.db=dbconn
        self.ses=self.db.Session()
        #creates the settings table if it doesn't exists
        if not self.db.dbeng.has_table('settings',schema='admin'):
            GSBase.metadata.create_all(self.db.dbeng)
            # #also grant geoslurp all privileges
            # #geoslurp users need to be able to add themselves to the admin.settings table
            self.db.dbeng.execute('GRANT ALL PRIVILEGES ON admin.settings to geoslurp')
            self.db.dbeng.execute('GRANT USAGE ON SEQUENCE admin.settings_id_seq to geoslurp')

        try:
            #extract the default entry
            self.defaultentry=self.ses.query(self.table).filter(self.table.user == 'default').one()
        except:
            #create a new default entry
            self.defaultentry=SettingsTable(user='default',conf={"CacheDir":"/tmp","MirrorMaps":{"default":"${HOME}/geoslurpdata"}})
            self.ses.add(self.defaultentry)
            self.ses.commit()
            #retrieve again
            self.defaultentry=self.ses.query(self.table).filter(self.table.user == 'default').one()
        #retrieve/create a user entry
        try:
            self.userentry=self.ses.query(self.table).filter(self.table.user == self.db.user).one()
        except:
            #make a new empty entry
            self.userentry=self.table(user=self.db.user,conf={})
            self.ses.add(self.userentry)
            self.ses.commit()

        self.decryptAuth()

        if dbconn.mirror:
            if dbconn.mirror != "default":
                #we need to apply a mapping of the directory names for this instance
                self.mirrorMap=MirrorMap(self.getMirror(dbconn.mirror),self.getMirror("default"))
        elif "MirrorMaps" in self.defaultentry.conf:
            #automatically try to figure out whether a mapping is needed by checking the existence of directories
            for alias,pth in self.defaultentry.conf["MirrorMaps"].items():
                if os.path.isdir(pth):
                    self.mirrorMap=MirrorMap(self.getMirror("default"),pth)
                    break
        # self.loaduserplugins()

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self, key):
        if key in self.userentry.conf:
            return self.userentry.conf[key]
        elif key in self.defaultentry.conf:
            #try finding the key in the defaults
            return self.defaultentry.conf[key]
        else:
            raise RuntimeError("Cannot find setting in user configuration or defaults")

    def __setitem__(self, key, val):
        self.userentry.conf[key]=val

    def __delitem__(self, key):
        del self.userentry.conf[key]

    def getdefaults(self,key):
        """returns the default"""
        return self.defaultentry.conf[key]

    def setdefault(self,key,val):
        self.defaultentry.conf[key]=val

    def show(self):
        """Show the loaded user configuration"""
        print(self.userentry.conf)
        #also shows the registered authentification services (but don't show passwords
        print(stripPasswords(self.auth))

    def authCred(self,service):
        """obtains username credentials for a certain service
        :param service: name of the service
        :returns a namedtuple with credentials"""

        if service not in self.auth:
            raise RuntimeError("No credentials for service %s found in the database. Please register your credentials using Settings.updateAuth()"%service)
        return Credentials(alias=service, **self.auth[service])

    def updateAuth(self, cred:Credentials):
        """register/update a new set of authentication credentials"""
        self.auth.update({cred.alias:dict([(ky, val) for ky, val in zip(cred._fields, cred) if bool(val) and ky != "alias"])})
        self.encryptAuth()
        self.ses.commit()

    def delAuth(self,key):
        """Delete an authentication entry by specifying it's alias"""
        del self.auth[key]
        self.encryptAuth()
        self.ses.commit()

    def update(self,indict=None):
        """Update the conf dictionary in postgresql settings table"""
        if indict:
            if self.userentry.conf:
                # check for none values and delete those entries
                for ky,val in indict.items():
                    if val == 'None':
                        try:
                            del self.userentry.conf[ky]
                        except KeyError:
                            slurplogger().warning("Cannot find key: %s in user configuration, ignoring.."%(ky))
                            pass
                    else:
                        self.userentry.conf[ky]=val

                # self.userentry.conf.update(indict)
            else:
                self.userentry.conf=indict


        self.ses.commit()
    
    def defaultupdate(self,indict=None):
        """Update the default conf dictionary in postgresql settings table"""
        if indict:
            if self.defaultentry.conf:
                self.defaultentry.conf.update(indict)
            else:
                self.defaultentry.conf=indict


        self.ses.commit()
    
    def getMirror(self,alias):
        if "MirrorMaps" in self.defaultentry.conf:
            return self.defaultentry.conf["MirrorMaps"][alias]
        else:
            return self.defaultentry.conf["DataDir"]

    def setMirror(self,alias,mirror):
        self.defaultupdate({"MirrorMaps":{alias:mirror}})

    def encryptAuth(self):
        """Encrypt the authentification credentials to store in the database"""
        bs = int(algorithms.Blowfish.block_size / 8)
        backend = default_backend()
        iv = os.urandom(bs)
        cipher = Cipher(algorithms.Blowfish(self.db.passw.encode('utf-8')), modes.CBC(iv), backend=backend)
        encryptor = cipher.encryptor()
        conf=json.dumps(self.auth).encode('utf-8')
        #padd with spaces to be a multiple of bs
        plen = bs - divmod(len(conf), bs)[1]
        pad = b' '*plen
        self.userentry.auth = iv + encryptor.update(conf+pad) + encryptor.finalize()
        # bs = Blowfish.block_size
        # iv=Random.new().read(bs)
        # crypto = Blowfish.new(self.db.passw, Blowfish.MODE_CBC, iv)
        # conf=json.dumps(self.auth)
        # #padd with spaces to be a multiple of bs
        # plen = bs - divmod(len(conf), bs)[1]
        # pad = ' '*plen
        # self.userentry.auth = iv + crypto.encrypt(conf + pad)
        #

    def decryptAuth(self):
        """Decrypt the authenficiation credentials as stored in the database"""
        if self.userentry.auth:
            bs=int(algorithms.Blowfish.block_size/8)
            backend = default_backend()
            iv = self.userentry.auth[0:bs]
            encr=self.userentry.auth[bs:]
            cipher = Cipher(algorithms.Blowfish(self.db.passw.encode('utf-8')), modes.CBC(iv), backend=backend)
            decryptor = cipher.decryptor()
            self.auth=json.loads(decryptor.update(encr) + decryptor.finalize())
        else:
            self.auth={}

        # bs = Blowfish.block_size
        # if self.userentry.auth:
        #     entry=self.userentry.auth
        #     iv=self.userentry.auth[0:bs]
        #     encr=self.userentry.auth[bs:]
        #     crypto = Blowfish.new(self.db.passw, Blowfish.MODE_CBC, iv)
        #     self.auth=json.loads(crypto.decrypt(encr))
        # else:
        #     #just empty
        #     self.auth={}
        #     return

    def getDataDir(self,scheme,dataset=None,subdirs=None):
        """Retrieves the data Directory, possibly appended with a dataset and subdirs"""
        #begin with setting the default
        ddir=self.getMirror("default")
        ddir=os.path.join(ddir,scheme)

        #Possibly we need to append a dataset directory 
        if dataset:
            ddir=os.path.join(ddir,dataset)

        #possibly append subdirectories
        if subdirs:
            ddir=os.path.join(ddir,subdirs)

        #NOTE: this posssibly applies a mapping of the root part of the directory 
        return getCreateDir(ddir,self.mirrorMap)

    def getCacheDir(self,scheme,dataset=None,subdirs=None):
        """Obtain and create a cache directory"""
        #starting point
        ddir=os.path.join(self.db.cache,scheme)

        # if "CacheDir" in self.userentry.conf:
        #     ddir=self.userentry.conf["CacheDir"]
        # else:
        #     ddir=self.defaultentry.conf["CacheDir"]
        #
        # ddir=os.path.join(ddir,scheme)

        #POssibly we need to append a dataset directory 
        if dataset:
            ddir=os.path.join(ddir,dataset)

        #possibly append subdirectories
        if subdirs:
            ddir=os.path.join(ddir,subdirs)

        #NOTE: this posssibly applies a mapping of the root part of the directory 
        return getCreateDir(ddir)

