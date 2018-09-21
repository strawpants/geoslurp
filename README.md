# Download and manage (geodetic) datasets with geoslurp
The idea is that this tool contains script to download/update commonly used datasets and to register them in a ~~mongodb~~ postgresql database. This database can then be queried allowing the retrieval of the relevant data or datafiles. 

# Philosophy
The idea behind geoslurp is to develop a hybrid approach to store data (either as URL's or file links) and corresponding scientific meta information which goes beyond the obvious. Geoslurp therefore has several aims:
* Have a centralized access point to scientific data (but the data itself may be stored in disctributed ways)
* Allow advanced queries on the metainfo data (e.g. time and spatial queries, data specific queries)
* Provide ways to automatically download/update/register datasets
* Store Unified Resource Identifiers to datasets instead of just files.
* Future ideas: Encourage reproducibility by registration of runs (e.g. store links to used datasets/queries and configuration scripts)
* REST-API to access from external

#FAQ
Q: Why use postgresql and not sqlite for a start?
A: I foresee uses of advanced postgis functionality (i.e. server based spatial queries), which can not be done with sqlite. Postgresql requires a database server to be run, which may be considered tedious. So this is the trade-off I made.

Status: Experimental:
Downloading and registering GSHHG and the Randalph Glacier inventory (6.0) data, and GRACE level 2 data seems to work now


## TODO
A lot
create a plugin for the podaac.jpl.nasa rest api? [see](https://podaac.jpl.nasa.gov/ws)
