
from geoslurp.datapull.webdav import Crawler as webdav
from geoslurp.config import SlurpConf
import os
conf=SlurpConf(os.path.join(os.path.expanduser('~'), '.geoslurp.yaml'))
cred=conf.authCred("podaac")
wb=webdav("https://podaac-tools.jpl.nasa.gov/drive/files/allData/grace/L2/CSR/RL06", pattern='GAC.*gz',auth=cred)


for uri in wb.uris():
    print(uri.url)
