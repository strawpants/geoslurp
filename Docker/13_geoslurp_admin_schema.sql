CREATE SCHEMA admin;

GRANT CREATE,USAGE ON SCHEMA admin to geoadmin;
GRANT USAGE ON SCHEMA admin to geoeditor;
GRANT USAGE ON SCHEMA admin to geobrowse;

ALTER DEFAULT PRIVILEGES IN SCHEMA admin GRANT SELECT,UPDATE,INSERT ON TABLES TO geoadmin;


CREATE TABLE admin.inventory (
	id UUID PRIMARY KEY DEFAULT uuidv7(),    
    scheme character varying,
    dataset character varying,
    pgfunc character varying,
    owner character varying,
    lastupdate timestamp without time zone,
    updatefreq integer,
    version integer[],
    cache character varying,
    datadir character varying,
    data jsonb,
    view character varying
);

ALTER TABLE admin.inventory OWNER TO geoadmin;



GRANT ALL PRIVILEGES ON admin.inventory to geoadmin;
GRANT SELECT,UPDATE,INSERT ON admin.inventory to geoeditor;
GRANT SELECT ON admin.inventory to geobrowse;

CREATE TABLE admin.users (
	id UUID PRIMARY KEY DEFAULT uuidv7(),    
    "user" character varying UNIQUE,
    conf jsonb,
    auth bytea
);

ALTER TABLE admin.users OWNER TO geoadmin;

ALTER TABLE admin.users ENABLE ROW LEVEL SECURITY;
CREATE POLICY self_mod ON admin.users TO geoeditor USING ( "user" = current_user);
CREATE POLICY browse_view ON admin.users FOR SELECT TO geobrowse USING ( "user" = current_user);


GRANT ALL PRIVILEGES ON admin.users to geoadmin;
GRANT SELECT,UPDATE,INSERT ON admin.users to geoeditor;
GRANT SELECT ON admin.users to geobrowse;







CREATE FUNCTION out_db_path() RETURNS text AS $$
                SELECT '/geo_outdb';
            $$ LANGUAGE SQL;

GRANT EXECUTE ON FUNCTION out_db_path TO geobrowse,geoeditor,geoadmin;

