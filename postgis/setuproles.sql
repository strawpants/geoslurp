/*This sets up an additional geoslurp (group) role which users will automatically made member of. This allows users to create there own schemes and to make queries on tables which are
 constructed by other users (unless they forcefully remove 'select' privileges themselves)*/
CREATE ROLE geoslurp;
GRANT CREATE ON DATABASE geoslurp TO geoslurp;
SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
ALTER DATABASE geoslurp SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
