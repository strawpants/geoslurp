.. _install:

Installation of the geoslurp package and setting up the PostgreSQL instance 
===========================================================================

Installation of the geoslurp package
------------------------------------

Currently the package is not yet in PyPI (but it hopefully will in the near future). Untill then, please clone the git repository and install using setuptools::

   git clone git@github.com:strawpants/geoslurp.git
   cd geoslurp
   python3 ./setup.py install

For a development install you can replace the final line with ``python3 ./setup.py develop``

Setting up the PostgreSQL database
----------------------------------
The (meta)data will be stored in a database. 


