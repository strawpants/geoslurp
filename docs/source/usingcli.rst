Using the command line tool geoslurp
====================================

A command line program is provided which can be used to register known datasets, and edit configuration settings. The full options are described in :ref:`geoslurphelp`, but the examples below can be a practical starting point for common tasks.

Getting info and help
---------------------
List the available help options on the command line::

        geoslurper --help

To get detailed help on the *pull* and *register* method of a certain dataset (some datasets may require specialized command line arguments here). One can combine the *help* option with a dataset::

    geoslurper --help altim.Duacs

A dataset should be specified in the form SCHEMA.DATASET

To list available datasets which are contained in the cached catalogue::

   geoslurper --list

To retrieve registration info on the current database holdings::

   geoslurper --info

Show the user's settings as stored in the database::

    geoslurper --show-config

Administrative tasks
--------------------


Default settings( available for all users, unless individually overruled) can be added to the database by users who have sufficient privileges. The option expects JSON arguments to specify the settings::

    geoslurper --admin-config '{"userplugins":"/home/geoslurp/geoslurpplugins"}'

User settings can also be added by providing JSON arguments::

    geoslurper --config '{"userplugins":"/home/roelof/customplugins"}'

Register authentication details for a specific service alias. For example for the copernicus Marine service one can specify::

    geoslurper --auth-config '{"cmems": {"user": "yourusername", "passw": "yoursupersecretpassword"}}'

Another example, needed for repetitive crawling of github repo's::

    geoslurper --auth-config '{"github": {"oauth": "OAUTHAPIKEY"}}'


Dataset Management
------------------
Datasets can be explicitly described by using the *-d* option in the form of *scheme.datasetclass*. However, the option also accepts a regular expression, so that multiple datasets can be selected at once.

To download the data needed for a specific dataset::

    geoslurper --pull oceanobs.Orsifronts

To delete the data belonging to a certain dataset::

    geoslurper --purge-data oceanobs.Orsifronts

To register a certain dataset in the database::

    geoslurper --register oceanobs.Orsifronts

To delete the database tables associated with the database::

    geoslurper --purge-entry oceanobs.Orsifronts

View management
---------------
TBD

Database Function Management
----------------------------
TBD






