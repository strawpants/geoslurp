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
from sqlalchemy.schema import CreateSchema, DropSchema, Table
from geoslurp.config import Log
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from geoslurp.db import Inventory, GSBase


def tableMapFactory(tableName, table=None):
    """Dynamically create/retrieve a mapper class from a Table object"""
    try:
        # possibly this class was already defined (we can only define it once)
        return globals()[tableName]
    except KeyError:
        if table != None:
            return type(tableName, (GSBase,), {'__table__': table})
        else:
            raise Exception('When creating a new SQLAlchemy tablemap, a table is needed')


class GeoslurpConnector():
    """Holds a connector to a geoslurp database"""

    def __init__(self, dburl):
        """
        establishes a database engine whoch provides the base
        for creating sessions (ORM) or connections (SQL expressions)
        :param dburl: url of the database e.g.: postgresql+psycopg2://geoslurp:password@host/geoslurp
        """
        self.dbeng = create_engine(dburl, echo=False)
        self.Session = sessionmaker(bind=self.dbeng)
        self.mdata = MetaData(bind=self.dbeng)
        self.inventTable = Inventory(self)

    def vacuumAnalyze(self, tableName, schema):
        """vacuum and analyze a certain table"""
        conn = self.dbeng.raw_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute('VACUUM ANALYZE %s."%s";' % (schema, tableName))

    def CreateSchema(self, schema):
        try:
            self.dbeng.execute(CreateSchema(schema.lower()))
        except:
            pass

    def dropSchema(self, schema, cascade=False):
        self.dbeng.execute(DropSchema(schema.lower(), cascade=cascade))

    def dropTable(self, tablename, schema):
        self.dbeng.execute('DROP TABLE IF EXISTS %s."%s";' % (schema, tablename))

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
