
# from geoslurp.datapull.webdav import Crawler as webdav
from geoslurp.datapull.rsync import Crawler as rscrawler
from geoslurp.config import SlurpConf
import os
conf=SlurpConf(os.path.join(os.path.expanduser('~'), '.geoslurp.yaml'))
cred=conf.authCred("rads")


# wb=webdav("https://podaac-tools.jpl.nasa.gov/drive/files/allData/grace/L2/CSR/RL06", pattern='GAC.*gz',auth=cred)
rsync=rscrawler(url="rads.tudelft.nl::rads/data/conf",auth=cred)
for file in rsync.parallelDownload('rads',True):
    print(file)
print('fnished')

# for uri in wb.uris():
#     print(uri.url)
