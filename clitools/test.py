
# from geoslurp.datapull.webdav import Crawler as webdav
from geoslurp.dataset import getNaturalEarthDict
from geoslurp.db import GeoslurpConnector,Inventory,Settings

DbConn=GeoslurpConnector("localhost","geoslurp","Allesoporde")
# Initializes an object which holds the current inventory
slurpInvent=Inventory(DbConn)
conf=Settings(DbConn,"geoslurp","Allesoporde")
dicts=getNaturalEarthDict(conf)

