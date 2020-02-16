.. _install:

===========================================================================
Installation of the geoslurp package and setting up the PostgreSQL instance 
===========================================================================

Geoslurp will only function when a PostGIS enabled database server is reachable. The python module geoslurp can be considered as a client and does not require the installation of a PostGreSQL database on the client machine itself. In this way, several hosts, with geoslurp installed as clients, can access the same database and data storage location, as indicated in the diagram below.

.. image:: graphics/geoslurp_network.svg
   :width: 600

Installation of the geoslurp package
====================================

Currently the package is not yet in PyPI (but it hopefully will in the near future). Untill then, please clone the git repository and install using setuptools::

   git clone git@github.com:strawpants/geoslurp.git
   cd geoslurp
   python3 ./setup.py install

For a development install you can replace the final line with ``python3 ./setup.py develop``

Setting up the PostgreSQL database
==================================
To setup the database one is (currently) referred to the documentation of `Running geoslurp with docker <https://github.com/strawpants/docker-geoslurp>`_. The basic steps are essentially to:

1. install a PostGreSQL instance with the PostGIS extension,
2. add a database called 'geoslurp',
3. set up geoslurp roles and users.

