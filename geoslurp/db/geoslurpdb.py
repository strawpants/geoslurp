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

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2019
from geoslurp.config.localsettings import readLocalSettings,settingsArgs
from geoslurp.db.connector import GeoslurpConnector


def geoslurpConnect(args=None,readonly_user=True,update=False,local_settings=None,dbalias=None):
    """Convenience wrapper to start a connection with a geoslurpdatabase. Returns a database connection while taking use of stored settings from the users 
    configuration file ($HOME/.geoslurp_lastused.yaml).
    :param args: Class which encapsulates different connection parameters
    :type args: geoslurp.localsettings.settingsArgs"""
   
    if not args:
        args=settingsArgs()

    if local_settings:
        args.local_settings=local_settings

    if dbalias:
        args.dbalias=dbalias
    
    userSettings=readLocalSettings(args=args,readonlyuser=readonly_user)
    return GeoslurpConnector(host=userSettings.host,user=userSettings.user,passwd=userSettings.password,
            cache=userSettings.cache,dataroot=userSettings.dataroot)
