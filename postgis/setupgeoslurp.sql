/*This sets up the geoslurp database and the default users */
CREATE DATABASE geoslurp
CREATE ROLE geoslurp;
GRANT CREATE ON DATABASE geoslurp TO geoslurp;
CREATE ROLE slurpbrowser;
GRANT SELECT ON DATABASE geoslurp TO slurpbrowser;
SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
ALTER DATABASE geoslurp SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
