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

from sqlalchemy import create_engine, MetaData,and_,inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists,select,column,table,text
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy import Table,func
from geoslurp.db.tabletools import tableMapFactory
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from geoslurp.config.slurplogger import  slurplogger, debugging
import getpass
from geoslurp.db.connectorbase import GeoslurpConnectorBase
import warnings
from sqlalchemy import exc as sa_exc


def tname(tablename,schema=None):
    """Create a fully qualified database name in lower case from a schema and tablename"""
    tnm=".".join(filter(None,[schema,tablename])).lower()
    return tnm

class GeoslurpConnector(GeoslurpConnectorBase):
    """Holds a connector to a geoslurp database"""
    def __init__(self, host, user, passwd=None, port=5432,dataroot=None,cache=None):
        """
        establishes a database engine whoch provides the base
        for creating sessions (ORM) or connections (SQL expressions)
        :param dburl: url of the database e.g.: postgresql+psycopg2://geoslurp:password@host/geoslurp
        """
        super().__init__(dataroot=dataroot,cache=cache)


        self.user=user
        if passwd:
            self.passw=passwd
        else:
            self.passw=getpass.getpass(prompt='Please enter password for %s: '%(user))

        if not host or host == 'unixsocket':
            #this will attempt to read from a unix socket
            self.host=""
        else:
            self.host=host
        

        echo=debugging()
        dburl="postgresql+psycopg2://"+user+":"+self.passw+"@"+self.host+":"+str(port)+"/geoslurp"
        self.dbeng = create_engine(dburl, echo=echo)
        self.Session = sessionmaker(bind=self.dbeng)

        self.mdata = MetaData()
        #reflect the public tables
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            self.mdata.reflect(bind=self.dbeng)
        if not self.schemaexists('admin'):
            raise RuntimeError("The database does not have an admin schema, is it properly initialized?")

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
        table=tname(tablename,schema)
        cursor.execute(text(f'VACUUM ANALYZE {table}";'))

    def CreateSchema(self, schema,private=False):
        with self.dbeng.connect() as conn:
            if private:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS %s;"%(schema.lower())))
            else:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION geoslurp;"%(schema.lower())))
                conn.execute(text("GRANT USAGE ON SCHEMA %s to geobrowse;"%((schema.lower()))))

                conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO geobrowse,geoslurp;"%((schema.lower()))))
                conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT USAGE ON SEQUENCES TO geobrowse,geoslurp;"%((schema.lower()))))
                conn.commit()

    def schemaexists(self,name):
        qry=exists(select(column("schema_name")).select_from(text("information_schema.schemata")).where(text("schema_name = '%s'"%(name))))
        return self.Session().query(qry).scalar()

    def dropSchema(self, schema, cascade=False):
        with self.dbeng.connect() as conn:
            conn.execute(DropSchema(schema.lower(), cascade=cascade))
            conn.commit()

    def createTable(self, tablename,columns,schema=None,temporary=False,truncate=False,bind=None):
        """Creates a (temporary) table from sqlalchemy columns and returns the corresponding tablemapper"""
        

        if bind is None:
            bind=self.dbeng
        mdata=self.mdata 
        # mdata.reflect(self.dbeng,schema=schema)


        if truncate:
            self.truncateTable(tablename,schema)

        if tablename in mdata.tables:
            table=mdata.tables[tablename]
        else:
            if temporary:
                table = Table(tablename, mdata, *columns, prefixes=['TEMPORARY'],postgresql_on_commit='PRESERVE ROWS')
            else:
                table = Table(tablename, mdata, *columns, schema=schema)

            table.create(bind=bind,checkfirst=True)

        return tableMapFactory(tablename, table)

    def truncateTable(self,tablename,schema=None):

        table=tname(tablename,schema)
        with self.dbeng.connect() as conn: 
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            conn.commit()

    def dropTable(self, tablename, schema=None):
        table=tname(tablename,schema)
        with self.dbeng.connect() as conn: 
            conn.execute(text(f'DROP TABLE IF EXISTS {table} CASCADE;'))
            conn.commit()

    def tableExists(self,tablename):
        insp=inspect(self.dbeng)
        sch,tbl=tablename.split(".")
        return insp.has_table(tbl,sch)


    def getTable(self,tname,schema="public",customcolumns=None):
        mdata=MetaData(bind=self.dbeng,schema=schema)
        if customcolumns:
            return Table(tname, mdata, *customcolumns,autoload=True, autoload_with=self.dbeng)
        else:
            return Table(tname, mdata, autoload=True, autoload_with=self.dbeng)

    def getFunc(self,fname,schema="public"):
        """returns a database function based up string names"""
        if schema == "public":
            return getattr(func,fname)
        else:
            return getattr(getattr(func,schema),fname)

    def createView(self, viewname, qry,schema=None):
        viewn=tname(viewname,schema)
        with self.dbeng.connect() as conn:
            conn.execute(text(f'CREATE VIEW {viewn} AS {qry};'))
            conn.commit()
    
    def dropView(self, viewname, schema=None):
        viewn=tname(viewname,schema)
        
        with self.dbeng.connect() as conn:
            conn.execute(text(f'DROP VIEW IF EXISTS {viewn};'))
            conn.commit()

    def addUser(self,name,passw,readonly=False):
        """Adds a user to the database (note executing this functions requires appropriate database rights"""
        slurplogger().info("Adding new user: %s"%(name))
        with self.dbeng.connect() as conn:
            if readonly:
                conn.execute(text(f"CREATE USER {name} WITH ENCRYPTED PASSWORD '{passw}' IN ROLE geobrowse;"))
            else:
                conn.execute(text(f"CREATE USER {name} WITH ENCRYPTED PASSWORD '{passw}' IN ROLE geoslurp,geobrowse;"))

            conn.commit()

    def execute(self,qry):
        with self.dbeng.connect() as conn:
            res=conn.execute(text(qry))
            conn.commit()
            return res
