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
import getpass

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# for the newer encryption
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

def getCreateDir(returndir):
    """creates a directory when not existent and return it"""

    #possibly expand environment variables in the string
    returnex=os.path.expandvars(returndir)

    if not os.path.exists(returnex):
        os.makedirs(returnex)
    return returnex

def stripPasswords(d):
    for serv,dsub in d.items():
        # print(serv)
        for ky,v in dsub.items():
            if ky == "passw" or ky == "oauthtoken":
                d[serv][ky]="*****"
    return d

Credentials=namedtuple("Credentials","user passw alias oauthtoken url ftptls trusted")
"""A named tuple to store authentication credentials

Attributes:
    user (str): Username for the service
    alias (str): (obligatory) The short name of this service
    passw (str): The password associated with the username
    oauthtoken (str): An oauth2 token
    url (str): The root url which is linked to this service
    ftptls(bool): Use Explicit ftp over tls for this host and user
    trusted(bool): Allow forwarding password and username
"""

Credentials.__new__.__defaults__ = (None,) * len(Credentials._fields)

commonCredentials={"podaac":["user","passw","trusted"]}






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
    pgmount=None
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
            self.defaultentry=SettingsTable(user='default',conf={})
            self.ses.add(self.defaultentry)
            self.ses.commit()
            #also create a view to the default which may be read by the geobrowse group
            self.db.dbeng.execute("CREATE VIEW admin.settings_default as select * from admin.settings as t where t.user  = 'default'")
            self.db.dbeng.execute("GRANT SELECT ON admin.settings_default to geobrowse")


            #retrieve again
            self.defaultentry=self.ses.query(self.table).filter(self.table.user == 'default').one()
        #retrieve/create a user entry
        try:
            self.userentry=self.ses.query(self.table).filter(self.table.user == self.db.user).one()
            if not self.userentry.conf:
                self.userentry.conf={}
        except:
            #make a new empty entry
            self.userentry=self.table(user=self.db.user,conf={})
            self.ses.add(self.userentry)
            self.ses.commit()

        if "pg_geoslurpmount" in self.defaultentry.conf:
            #retrieve the dataroot to which the postgresql instance itself has access (e.g. for out-db-raster)
            self.pgmount=self.defaultentry.conf["pg_geoslurpmount"]

        self.decryptAuth()

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

    def show(self,hideflag="hide"):
        """Show the loaded user configuration"""
        print(self.userentry.conf)
        #also shows the registered authentification services (but don't show passwords
        if hideflag == "nohide":
            print(self.auth)
        else:
            print(stripPasswords(self.auth))

    def authCred(self,service,qryfields=None):
        """obtains username credentials for a certain service
        :param service: name of the service
        :param qryfields: prompt for input for these fields when service is not present
        :returns a namedtuple with credentials"""

        if service not in self.auth:
            #prompt for the necessary fields and store in database
            if qryfields is not None and service in commonCredentials:
                qryfields=commonCredentials[service]
            else:
                qryfields=["user","passw"]

            creddict={"alias":service}
            for key in qryfields:
                if key == "passw":
                    val=getpass.getpass(prompt='Please enter passw for authentication service %s\n'%service)
                else:
                    val=input('Please enter %s for authentication service %s\n'%(key,service))
                creddict[key]=val
            self.updateAuth(cred=Credentials(**creddict))
            
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
    
    def get_PG_path(self,url):
        """ Possibly modifies a path so it becomes a path accessible by the Database host itself"""
        if self.pgmount:
            if not url.startswith("/"):
                #just prepend to a relative path
                url=os.path.join(self.pgmount,url)
            else:
                #try modifying the path
                url=url.replace(self.db.localdataroot,self.pgmount)
        return url
    
    def get_local_path(self,url):
        """possibly prepend the localdataroot root to the path or"""
        #possibly subsitute localdataroot placeholder
        url=url.replace("${LOCALDATAROOT}",self.db.localdataroot)

        if not url.startswith("/"):
            url=os.path.join(self.db.localdataroot,url)

        return url
    
    def generalize_path(self,url):
        return url.replace(self.db.localdataroot,"${LOCALDATAROOT}")

    def encryptAuth(self):
        """Encrypt the authentification credentials to store in the database"""
        salt = os.urandom(16)
        cyph=self.genCypher(salt,self.db.passw.encode('utf-8'))

        conf=json.dumps(self.auth).encode('utf-8')
        
        if self.authver== "ENCRV1":
            slurplogger().warning("Replacing the authentication details with a safer encryption (not compatible with older geoslurp versions")
            self.authver="ENCRV2"
        self.userentry.auth = self.authver.encode('utf-8') + salt +  cyph.encrypt(conf)  
        return
    
    def decryptAuthv1(self):
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

    def decryptAuth(self):
        """Decrypt the authenfication credentials as stored in the database""" 
        self.authver="ENCRV2"
        if self.userentry.auth:
            shft=6
            if self.userentry.auth[0:shft] == b"ENCRV2":
                salt = self.userentry.auth[shft:shft+16]
                cyph=self.genCypher(salt,self.db.passw.encode('utf-8'))
                self.auth=json.loads(cyph.decrypt(self.userentry.auth[shft+16:]))
            else:
                #rollback encryption version
                self.authver="ENCRV1"
                #decrypt using the old version
                self.decryptAuthv1()
        else:
            self.auth={}
    
    @staticmethod
    def genCypher(salt,password):
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),length=32,salt=salt,iterations=100000)
        return Fernet(base64.urlsafe_b64encode(kdf.derive(password)))

    def getDataDir(self,scheme,dataset=None,subdirs=None):
        """Retrieves the data Directory, possibly appended with a dataset and subdirs"""
        #begin with setting the default
        ddir=self.db.localdataroot

        ddir=os.path.join(ddir,scheme)

        #Possibly we need to append a dataset directory 
        if dataset:
            ddir=os.path.join(ddir,dataset)

        #possibly append subdirectories
        if subdirs:
            ddir=os.path.join(ddir,subdirs)

        return getCreateDir(ddir)

    def getCacheDir(self,scheme,dataset=None,subdirs=None):
        """Obtain and create a cache directory"""
        #starting point
        ddir=os.path.join(self.db.cache,scheme)

        #POssibly we need to append a dataset directory 
        if dataset:
            ddir=os.path.join(ddir,dataset)

        #possibly append subdirectories
        if subdirs:
            ddir=os.path.join(ddir,subdirs)

        #NOTE: this posssibly applies a mapping of the root part of the directory 
        return getCreateDir(ddir)

