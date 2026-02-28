CREATE ROLE geoeditor;
CREATE ROLE geobrowse; 
CREATE ROLE geoadmin WITH CREATEROLE;

GRANT CREATE ON DATABASE geoslurp TO geoadmin;
GRANT CREATE ON DATABASE geoslurp TO geoeditor;
GRANT CONNECT ON DATABASE geoslurp TO geobrowse;

GRANT geobrowse to geoadmin WITH ADMIN OPTION; 
GRANT geoeditor to geoadmin WITH ADMIN OPTION; 


