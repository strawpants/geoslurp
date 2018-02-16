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

import os.path
import yaml
import sys

class slurpconf():
    """ Class which reads and writes configure data in yaml format"""
    def __init__(self):
        self.confyaml=os.path.join(os.path.expanduser('~'),'.geoslurp.yaml')
        if os.path.exists(self.confyaml):
            #Read parameters from yaml file
            fid=open(self.confyaml,'r')
            self.confobj=[x for x in yaml.safe_load_all(fid)]
            fid.close()
    def default(self,out=sys.stderr):
        """Write the default configuration """
        obj={"Mongo":"mongodb://localhost:27017/","DataDir":"/tmp/geoslurp","PluginDir":"/tmp/geoslurp/plugins"}
        out.write("# Default configuration file for geoslurp\n")
        out.write("# Change settings below and save file to .geoslurp.yaml\n")
        yaml.dump(obj,out,default_flow_style=False)

    def write(self):
        """Writes changed setup back to confuguration file"""
        fid=open(self.confyaml,'w')
        yaml.dump(self.confobj[0],fid,default_flow_style=False)
        fid.close()

    #The operators below overload the [] operators allowing the retrieval and  setting of dictionary items
    def __getitem__(self,arg):
        return self.confobj[0][arg]

    def __setitem__(self,key,val):
        self.confobj[0][key]=val

