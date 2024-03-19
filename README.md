# Download and manage datasets in a PostGreSQL database with the PostGIS

[![Build and Publish](https://github.com/strawpants/geoslurp/actions/workflows/python-publish.yml/badge.svg)](https://github.com/strawpants/geoslurp/actions/workflows/python-publish.yml)
[![PyPI version](https://badge.fury.io/py/geoslurp.svg)](https://badge.fury.io/py/geoslurp)
[![Documentation Status](https://readthedocs.org/projects/geoslurp/badge/?version=latest)](https://geoslurp.wobbly.earth/latest/?badge=latest)
The idea is that this tool contains script to download (i.e. **slurp**) commonly used datasets and to register them in a postgresql+postgis database. This database can then be queried allowing the retrieval of the relevant data or datafiles. 

The main documentation lives at [geoslurp.wobbly.earth](https://geoslurp.wobbly.earth)

## Workings

The geoslurp module itself is a pure python module, which acts as a client. For this to work one needs to [set up a running PostGreSQL database](https://github.com/strawpants/docker-geoslurp).

![Image of geoslurp clients versus database server](docs/source/_static/geoslurp_network.svg)

## Change log V3
* Use python entry points to register datasets, views and functions (removes registration of user plugin directories)
* Move documentation to [https://readthedocs.org](https://geoslurp.wobbly.earth)
* Change github actions to pusblish and then release
* Work with SQAlchemy 2
* more (see commit messages)

## TODO's
* Move remaining 'orphaned' datasets to suitable python packages modules or include in the main package
* Improve this README
