.. geoslurp documentation master file, created by
   sphinx-quickstart on Mon Mar  4 20:45:17 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Data management with geoslurp
=============================

Philosophy
----------
The idea behind geoslurp is to develop a hybrid approach to store data (either as URL's or file links) and corresponding scientific meta information which goes beyond the obvious. Geoslurp therefore has several aims:
* Have a centralized access point to scientific data (but the data itself may be stored in disctributed ways)
* Allow advanced queries on the metainfo data (e.g. time and spatial queries, data specific queries)
* Provide ways to automatically download/update/register datasets
* Future ideas: Encourage reproducibility by registration of runs (e.g. store links to used datasets/queries and configuration scripts)
* REST-API to access from external



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation 
   clitools
   apigeoslurp/geoslurp

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
