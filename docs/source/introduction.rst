An introduction to Geoslurp
===========================

**Geoslurp is a  pure-python module which allows downloading/updating/querying of different datasets in a spatially aware database (PostgreSQL with PostGIS).**


The idea behind Geoslurp is to centrally manage datasets, which has the following advantages:

- It provides and encourages a central point to access to various datasets
- Established database functionatility can be exploited by allowing (spatial) queries of the datasets, *joins* (querying a combination of datasets)
- Sharing is encouraged, users share data by default, making it possible for other users to use the registered data and avoiding copies of large datasets
- It allows for consistent versioning of datasets
- The use of an 'off-the-shelf' database provides a standard and mature interface reachable from various programming languages.
- Large datafiles don't necessarily need to be in the database. The metainformation, required for the queries, will be stored together with a link to where the data can be found (e.g. a local file path or an online url). This allows relatively light-weight databases to be set up. 
- Provide ways to standardize downloading/updating/registering of datasets

Requirements
------------
To use Geoslurp one must essentially :ref:`install <install>` the Geoslurp Python package, and have a running PostgreSQL database with PostGIS. Geoslurp is currently not yet in PyPI, but this is foreseen in the future once a relatively decent first version is developed. 


What the future holds
-----------------------
At the time of writing, the geoslurp tools are still under development. But taking a look in the future, one may already philosophize about possible features:

- Manage a geoslurp instance by means of a website-frontend. Currently, the main way to interact with geoslurp is to use the :ref:`geoslurper command line script <geoslurphelp>`. This may be too cumbersome to learn for occasional users.
- Hide the database access behind a REST-API. This may be more secure when public instances will be set up and will facilitate the interaction with other webservices.
- Encourage reproducibility by registering scientific processing pipelines. 
