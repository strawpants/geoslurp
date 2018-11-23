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
from Crypto.Cipher import Blowfish
from Crypto import Random
import json
import os
from collections import namedtuple

def getCreateDir(returndir):
    """creates a directory when not existent and return it"""
    if not os.path.exists(returndir):
        os.makedirs(returndir)
    return returndir

Credentials=namedtuple("Credentials","user passw alias")

GSBase=declarative_base()

class SettingsTable(GSBase):
    """Defines the GEOSLURP POSTGRESQL inventory table"""
    __tablename__='settings'
    id=Column(Integer, primary_key=True)
    user=Column(String, unique=True)
    conf=Column(MutableDict.as_mutable(JSONB))
    auth=Column(BYTEA) # stored as blowfish encrypted bytearray


class Settings():
    """Read and write default and user specific settings to and from the database"""
    table=SettingsTable
    def __init__(self,dbconn,user,passwd):
        self.db=dbconn
        self.ses=self.db.Session()
        self.user=user
        self.passwd=passwd
        #creates the settings table if it doesn't exists
        if not self.db.dbeng.has_table('settings'):
            GSBase.metadata.create_all(self.db.dbeng)


        try:
            self.userentry=self.ses.query(self.table).filter(self.table.user == user).one()
        except:
            #make a new empty entry
            self.userentry=self.table(user=self.user)
            self.ses.add(self.userentry)
            self.ses.commit()

        self.decryptAuth()

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self, key):
        return self.userentry.conf[key]

    def __setitem__(self, key, val):
        self.userentry.conf[key]=val

    def authCred(self,service):
        """obtains username credentials for a certain service
        :param service: name of the service
        :returns a namedtuple with credentials"""
        return Credentials(alias=service, **self.auth[service])

    def updateAuth(self, indict):
        self.auth.update(indict)
        self.encryptAuth()
        self.ses.commit()

    def addUser(self,name,passw):
        pass

    def update(self,indict=None):
        """Update the conf dictionary in postgresql settings table"""
        if indict:
            if self.userentry.conf:
                self.userentry.conf.update(indict)
            else:
                self.userentry.conf=indict


        self.ses.commit()

    def encryptAuth(self):
        """Encrypt the authentification credentials to store in the database"""

        bs = Blowfish.block_size
        iv=Random.new().read(bs)
        crypto = Blowfish.new(self.passwd, Blowfish.MODE_CBC, iv)
        conf=json.dumps(self.auth)
        #padd with spaces to be a multiple of bs
        plen = bs - divmod(len(conf), bs)[1]
        pad = ' '*plen
        self.userentry.auth = iv + crypto.encrypt(conf + pad)


    def decryptAuth(self):
        """Decrypt the authenficiation credentials as stored in the database"""
        bs = Blowfish.block_size
        if self.userentry.auth:
            entry=self.userentry.auth
            iv=self.userentry.auth[0:bs]
            encr=self.userentry.auth[bs:]
            crypto = Blowfish.new(self.passwd, Blowfish.MODE_CBC, iv)
            self.auth=json.loads(crypto.decrypt(encr))
        else:
            #just empty
            self.auth={}
            return

    def getDir(self,scheme, dirEntry, dataset=None,subdirs=None):
        """
        :param scheme str: name of the database scheme
        :param dataset str: name of the dataset
        :param dirEntry: type of the directory to look for (CacheDir, or DataDir)
        :return: the directory (possibly created when it doesn't exist)
        """
        #begin with setting the default
        dirpath=getCreateDir(os.path.join(self.userentry.conf[dirEntry],scheme))

        #let's see if there is a specialized 'DataDir' entry for the dataset
        if dataset:
            try:
                dsetpath=getCreateDir(self.userentry.conf[scheme][dataset][dirEntry])
                #upon success let's add a symbolic link in the scheme datadir
                try:
                    os.symlink(dsetpath,os.path.join(dirpath,dataset))
                except:
                    #ok when it already exists
                    pass
                dirpath=dsetpath
            except KeyError:
                # no problem we can just stick with the default
                dirpath=getCreateDir(os.path.join(dirpath,dataset))

        #possibly append subdirectories
        if subdirs:
            dirpath=getCreateDir(os.path.join(dirpath,subdirs))

        return dirpath
