Using the command line tool geoslurp
====================================

A command line program is provided which can be used to register known datasets, and edit configuration settings. The full options are described in :ref:`geoslurphelp`, but the examples below can be a practical starting point for common tasks.

Getting info and help
---------------------
List the available help options on the command line::

        geoslurper.py --help

To get detailed help on the *pull* and *register* method of a certain dataset (some datasets may require specialized command line arguments here). One can combine the *help* option with the *-d* dataset option::

    geoslurper.py --help -d altim.Duacs

To list available datasets which are contained in the cached catalogue::

   geoslurper.py --list

To retrieve registration info on the current database holdings::

   geoslurper.py --info

Show the user's settings as stored in the database::

    geoslurper.py --show-config

Administrative tasks
--------------------

Whenever new dataset classes are added to the geoslurp/geoslurp/dataset or the user's plugin path (usersettings variable `userplugins`) one should explicitly refresh the dataset catalogue::

    geoslurper.py --refresh

Default settings( available for all users, unless individually overruled) can be added to the database by users who have sufficient privileges. The option expects JSON arguments to specify the settings::

    geoslurper.py --admin-config '{"userplugins":"/home/geoslurp/geoslurpplugins"}'

User settings can also be added by providing JSON arguments::

    geoslurper.py --config '{"userplugins":"/home/roelof/customplugins"}'

Register authentication details for a specific service alias. For example for the copernicus Marine service one can specify::

    geoslurper.py --auth-config '{"cmems": {"user": "yourusername", "passw": "yoursupersecretpassword"}}'

Another example, needed for repetitive crawling of github repo's::

    geoslurper.py --auth-config '{"github": {"oauth": "OAUTHAPIKEY"}}'


Dataset Management
------------------
Datasets can be explicitly described by using the *-d* option in the form of *scheme.datasetclass*. However, the option also accepts a regular expression, so that multiple datasets can be selected at once.

To download the data needed for a specific dataset::

    geoslurper.py --pull -d oceanobs.Orsifronts

To delete the data belonging to a certain dataset::

    geoslurper.py --purge-data -d oceanobs.Orsifronts

To register a certain dataset in the database::

    geoslurper.py --register -d oceanobs.Orsifronts

To delete the database tables associated with the database::

    geoslurper.py --purge-entry -d oceanobs.Orsifronts


