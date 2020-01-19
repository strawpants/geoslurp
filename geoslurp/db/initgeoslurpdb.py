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

from geoslurp.db.inventory import Inventory
from geoslurp.db.settings import Settings

# from geoslurp.db.inventory import GSBase as gsbinv
# from geoslurp.db.settings import SettingsTable
# from geoslurp.db.settings import GSBase as gsbset
#test

def initgeoslurpdb(conn):
    """Initiates main geoslurp admin structure"""
    #check if the admin schema exists and initiate this otherwise (note the first user who creates this owns it, so better make this an admin user!!)
    if conn.schemaexists('admin'):
        #quick return if the schema already exists
        return

    conn.CreateSchema('admin')

    #initializes the inventory table so it gets created
    inv=Inventory(conn)


    #initializes the settings table so it gets created
    conf=Settings(conn)

    # gsbinv.metadata.create_all(conn.dbeng)
    # conn.dbeng.execute('GRANT ALL PRIVILEGES ON admin.inventory to geoslurp;')
    # conn.dbeng.execute('GRANT USAGE ON SEQUENCE admin.inventory_id_seq to geoslurp')
    
    # #create settings table
    # gsbset.metadata.create_all(conn.dbeng)
    # conn.dbeng.execute('GRANT ALL PRIVILEGES ON admin.settings to geoslurp')
    # conn.dbeng.execute('GRANT USAGE ON SEQUENCE admin.settings_id_seq to geoslurp')

    #create a 'default' entry in the settings table
    defaultentry=SettingsTable(user='default',conf={"CacheDir":"/tmp","MirrorMaps":{"default":"${HOME}/geoslurpdata"}})

    # ses=conn.Session()
    # ses.add(defaultentry)
    # ses.commit()
    #allow the geoslurp user to access the sequence generator

