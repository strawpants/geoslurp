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

Local Configuration settings
============================
The settings which are specific to a client are stored in a yaml file which is expected in the user's home when not indicated otherwise. The file is called ``.geoslurp_lastused.yaml`` and may contain the following entries::

   host: hostname_of_database_server
   lastupdate: 2020-02-06 19:30:43.292675
   port: 5432 #where to contact the server
   readonlyUser: slurpy # user which has readonly access to the database (This is the preferred user to use when browsing and using the database but not for registering)
   useKeyring: true #This allows users to store their database password in a system keyring (e.g. kwallet, gnome-keyring, ..) Requires the python package keyring with a sutable backend 
   user: username #username which also has write access to the database

Note that settings in this file may be overwritten with last used values when using the :ref:`Command line tools <clihelp>`.



Specifying passwords
--------------------
There are several ways to specify passwords, some which are more secure than others. The follin 

1. **Use a keyring recognized by python keyring**. This requires that `python keyring  is installed with a suitable backend <https://pypi.org/project/keyring/>`_. When the option ``useKeyring=true`` is specified in ``${HOME}/.geoslurp_lastused.yaml``, users will be prompted for a  password when they connect for the first time, but subsequent accesses don't require entering a password, and are therefore non-interactive.
2. **Use an environment variable**. If ``useKeyRing=false``, one can use an environment variable to specify the password e.g.: ``export GEOSLURP_PGPASS="supersecretpassword``, or ``export GEOSLURP_PGPASSRO`` (for the readonly user).
3. If both methods 1 and 2 fail. one will be interactively prompted for a password
4. As an alternative the constructor of the `GeoslurpConnector <reference/geoslurp.db.html#geoslurp.db.connector.GeoslurpConnector>`_ class allows the explicit input of a password.


