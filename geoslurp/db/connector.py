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

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists,select,column,table,text
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy import Table
from geoslurp.db.tabletools import tableMapFactory
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from geoslurp.config.slurplogger import  slurplogger, debugging
import getpass

class GeoslurpConnector():
    """Holds a connector to a geoslurp database"""
    mirror=None
    cache=None
    def __init__(self, host, user, passwd=None, port=5432,readonlyuser=True,datamirror=None,cache=None):
        """
        establishes a database engine whoch provides the base
        for creating sessions (ORM) or connections (SQL expressions)
        :param dburl: url of the database e.g.: postgresql+psycopg2://geoslurp:password@host/geoslurp
        """
        self.user=user
        if passwd:
            self.passw=passwd
        else:
            self.passw=getpass.getpass(prompt='Please enter password for %s: '%(user))

        if not host:
            #this will attempt to read from a unix socket
            host=""
        self.host=host
        echo=debugging()
        dburl="postgresql+psycopg2://"+user+":"+self.passw+"@"+host+":"+str(port)+"/geoslurp"
        self.dbeng = create_engine(dburl, echo=echo)
        # self.conn=self.dbeng.connect()
        self.Session = sessionmaker(bind=self.dbeng)
        self.mdata = MetaData(bind=self.dbeng)

        if not self.schemaexists('admin'):
            raise RuntimeError("The database does not have an admin scheme, is it properly initialized?")

        if datamirror:
            self.mirror=datamirror

        if cache:
            self.cache=cache
        else:
            #default when not specified
            self.cache="/tmp/geoslurp_cache"


    def transsession(self):
        """Retrieve a  session which is bound to a connection rather than an engine (e.g. useful for temporary tables)"""
        conn=self.dbeng.connect()
        trans=conn.begin()
        return trans,self.Session(bind=conn)

    def vacuumAnalyze(self, tableName, schema):
        """vacuum and analyze a certain table"""
        conn = self.dbeng.raw_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute('VACUUM ANALYZE %s."%s";' % (schema, tableName))

    def CreateSchema(self, schema,private=False):
        if private:
            self.dbeng.execute("CREATE SCHEMA IF NOT EXISTS %s;"%(schema.lower()))
        else:
            self.dbeng.execute("CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION geoslurp;"%(schema.lower()))
            self.dbeng.execute("GRANT USAGE ON SCHEMA %s to geobrowse;"%((schema.lower())))

            self.dbeng.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO geobrowse,geoslurp;"%((schema.lower())))
            self.dbeng.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT USAGE ON SEQUENCES TO geobrowse,geoslurp;"%((schema.lower())))

    def schemaexists(self,name):
        return self.Session().query(exists(select([column("schema_name")]).select_from(text("information_schema.schemata")).where(text("schema_name = '%s'"%(name))))).scalar()

    def dropSchema(self, schema, cascade=False):
        self.dbeng.execute(DropSchema(schema.lower(), cascade=cascade))

    def createTable(self, tablename,columns,scheme=None,temporary=False,truncate=False,bind=None):
        """Creates a (temporary) table from sqlalchemy columns and returns the corresponding tablemapper"""

        if bind:
            mdata=MetaData(bind=bind)
        else:
            bind=self.dbeng
            mdata=self.mdata


        if truncate:
            self.truncateTable(tablename,scheme)

        if tablename in mdata.tables:
            table=mdata.tables[tablename]
        else:
            if temporary:
                table = Table(tablename, mdata, *columns, prefixes=['TEMPORARY'],postgresql_on_commit='PRESERVE ROWS')
            else:
                table = Table(tablename, mdata, *columns, schema=scheme)

            table.create(bind=bind,checkfirst=True)

        return tableMapFactory(tablename, table)

    def truncateTable(self,tablename,scheme=None):
        if scheme:
            self.dbeng.execute("TRUNCATE TABLE %s.%s;"%(scheme,tablename))
        else:
            self.dbeng.execute("TRUNCATE TABLE %s;"%(tablename))

    def dropTable(self, tablename, schema=None):
        if schema:
            self.dbeng.execute('DROP TABLE IF EXISTS %s."%s";' % (schema.lower(), tablename.lower()))
        else:
            self.dbeng.execute('DROP TABLE IF EXISTS "%s";' % (tablename.lower()))

    def getTable(self,tname,scheme="public"):
        mdata=MetaData(bind=self.dbeng,schema=scheme)
        return Table(tname, mdata, autoload=True, autoload_with=self.dbeng)

    def updateFunction(self, fname, schema, inpara, outtype, body, language):
        """Updates a stored function"""
        # explicitly add line endings after semicolons in the body when not done already
        body = re.sub(';(?!\n)', ';\n ', body)
        self.dbeng.execute('DROP FUNCTION IF EXISTS %s."%s";' % (schema, fname))
        funccmd = 'CREATE OR REPLACE FUNCTION %s.%s (%s) RETURNS %s AS $$ %s $$ LANGUAGE %s ' % (
        schema, fname, inpara, outtype, body, language)
        self.dbeng.execute(funccmd)

    def updateTable(self, TableContentGenerator):
        """Update/add row entries in a table"""

        #extract table columnn

        #possibly create table (if it does not exist)

        #loop over entries of the TableContentGenerator
        pass

    def addUser(self,name,passw,readonly=False):
        """Adds a user to the database (note executing this functions requires appropriate database rights"""
        slurplogger().info("Adding new user: %s"%(name))
        if readonly:
            self.dbeng.execute("CREATE USER %s WITH ENCRYPTED PASSWORD '%s' IN ROLE geobrowse;"%(name,passw))
        else:
            self.dbeng.execute("CREATE USER %s WITH ENCRYPTED PASSWORD '%s' IN ROLE geoslurp;"%(name,passw))





