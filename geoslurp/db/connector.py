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
from geoslurp.db.initgeoslurpdb import initgeoslurpdb
from geoslurp.db.tabletools import tableMapFactory
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from geoslurp.config.slurplogger import  slurplogger

class GeoslurpConnector():
    """Holds a connector to a geoslurp database"""

    def __init__(self, host, user, passwd):
        """
        establishes a database engine whoch provides the base
        for creating sessions (ORM) or connections (SQL expressions)
        :param dburl: url of the database e.g.: postgresql+psycopg2://geoslurp:password@host/geoslurp
        """
        self.user=user
        self.passw=passwd
        self.host=host
        dburl="postgresql+psycopg2://"+user+":"+passwd+"@"+host+"/geoslurp"
        self.dbeng = create_engine(dburl, echo=False)
        self.Session = sessionmaker(bind=self.dbeng)
        self.mdata = MetaData(bind=self.dbeng)
        initgeoslurpdb(self)


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
            self.dbeng.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TABLES TO geoslurp;"%((schema.lower())))
            self.dbeng.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT USAGE ON SEQUENCES TO geoslurp;"%((schema.lower())))

    def schemaexists(self,name):
        return self.Session().query(exists(select([column("schema_name")]).select_from(text("information_schema.schemata")).where(text("schema_name = '%s'"%(name))))).scalar()

    def dropSchema(self, schema, cascade=False):
        self.dbeng.execute(DropSchema(schema.lower(), cascade=cascade))

    def createTable(self, tablename,columns,scheme=None,temporary=False,truncate=False):
        """Creates a (temporary) table from sqlalchemy columns and returns the corresponding tablemapper"""

        if truncate:
            self.dbeng.execute("TRUNCATE TABLE %s;"%(tablename))

        if tablename in self.mdata.tables:
            table=self.mdata.tables[tablename]
        else:
            if temporary:
                table = Table(tablename, self.mdata, *columns, prefixes=['TEMPORARY'],postgresql_on_commit='PRESERVE ROWS')
            else:
                table = Table(tablename, self.mdata, *columns, schema=scheme)
            
            table.create(bind=self.dbeng,checkfirst=True)

        return tableMapFactory(tablename, table)


    def dropTable(self, tablename, schema=None):
        if schema:
            self.dbeng.execute('DROP TABLE IF EXISTS %s."%s";' % (schema.lower(), tablename.lower()))
        else:
            self.dbeng.execute('DROP TABLE IF EXISTS "%s";' % (tablename.lower()))

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

    def addUser(self,name,passw):
        """Adds a user to the database (note executing this functions requires appropriate database rights"""
        slurplogger().info("Adding new user: %s"%(name))
        self.dbeng.execute("CREATE USER %s WITH ENCRYPTED PASSWORD '%s' IN ROLE geoslurp;"%(name,passw))
