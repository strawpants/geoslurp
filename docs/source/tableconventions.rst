.. _tablecols:

===============================================
Conventions on naming tables and their  columns
===============================================

Tables and schemes are/should be named in lowercase notation where '_' can be used as separators.

The names of the columns are in principle free to choose, but an adherence to often-used columns according to the convention below is desired.

**geoslurp column naming conventions**

+-----------------+------------------+-----------------------------------------------------------------------+
| name            | PostGreSQL type  | Description                                                           |
+=================+==================+=======================================================================+
| uri             | TEXT             | | Unified resource identifier. This holds the location of the datafile|
|                 |                  | | It is usually a file name, but can also be an hyperlink             |
+-----------------+------------------+-----------------------------------------------------------------------+
| name            | TEXT             |  Unique shortname which identifies the entry in a row                 |
+-----------------+------------------+-----------------------------------------------------------------------+
| time            || TIMESTAMP or    | | Epoch relevant to the data row this may also be an array of         | 
|                 || ARRAY(TIMESTAMP)| | timestamps                                                          |
+-----------------+------------------+-----------------------------------------------------------------------+
| geom            | POSTGIS geometry | | Holds the geometry object which can be understood by                |
|                 |                  | | PostGIS                                                             |
+-----------------+------------------+-----------------------------------------------------------------------+
| rast            | POSTGIS raster   | Holds the raster object which can be understood by PostGIS            |
+-----------------+------------------+-----------------------------------------------------------------------+
| data            | JSONB            | Holds auxiliary data in the form of a json object                     |
+-----------------+------------------+-----------------------------------------------------------------------+

