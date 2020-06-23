=============
Configuration
=============


Local (client-side) settings
============================

The settings which are specific to a client are stored in a yaml file which is expected in the user's home when not indicated otherwise. The file is called ``.geoslurp_lastused.yaml`` and may contain the following entries::

   host: hostname_of_database_server
   lastupdate: 2020-02-06 19:30:43.292675
   port: 5432 #where to contact the server
   readonlyUser: slurpy # user which has readonly access to the database (This is the preferred user to use when browsing and using the database but not for registering)
   useKeyring: true #This allows users to store their database password in a system keyring (e.g. kwallet, gnome-keyring, ..) Requires the python package keyring with a sutable backend 
   user: username #username which also has write access to the database
   cache: /path/to/client/cachedir #One can explicitly specify a cache directoryfor the client here (e.g. on a fast ssd drive)
Note that settings in this file may be overwritten with last used values when using the :ref:`Command line tools <clihelp>`.

Populate a new local settings file
----------------------------------
The command line client can be used to generate a local settings file with settings specified on the command line e.g.:
``
geoslurper.py --host dbserverhost --user johndoe --keyring --write-local-settings 
``
This will create a file ``${HOME}/.geoslurp_lastused.yaml`` with the specified settings, and prompt the user for the database password.

Specifying passwords
--------------------
There are several ways to specify passwords, some which are more secure than others. The following options are available.

1. **Use a keyring recognized by python keyring**. This requires that `python keyring  is installed with a suitable backend <https://pypi.org/project/keyring/>`_. When the option ``useKeyring=true`` is specified in ``${HOME}/.geoslurp_lastused.yaml``, users will be prompted for a  password when they connect for the first time, but subsequent accesses don't require entering a password, and are therefore non-interactive.
2. **Use an environment variable**. If ``useKeyRing=false``, one can use an environment variable to specify the password e.g.: ``export GEOSLURP_PGPASS="supersecretpassword``, or ``export GEOSLURP_PGPASSRO`` (for the readonly user).
3. If both methods 1 and 2 fail. one will be interactively prompted for a password
4. As an alternative the constructor of the `GeoslurpConnector <reference/geoslurp.db.html#geoslurp.db.connector.GeoslurpConnector>`_ class allows the explicit input of a password.


Server-side settings
====================
The database table *admin.settings* contains a row with default settings and a user specific row which potentially overrule the defaults. The settings table contains, beside the usernames, a jsonb column *conf* and an encrypted bytestring *auth*, which contains sensitive login details.

Important configuration parameters
----------------------------------

1. **DataDir**:This is the root directory where out-of-db datafiles will be stored. This can be overruled for individual datasets (e.g. when the files belonging to a datase are already downloaded and available)

2. **CacheDir**: Temporary files will be stored in this directory. Note that this can be overruled per user.

3. **userplugins**: A list of semi-colon separated directories where custom *dataset* classes are stored. These directories need to contain a ``__init__.py`` file so it can be imported as a module.

