FROM postgis/postgis:18-3.6-alpine
MAINTAINER Roelof Rietbroek <roelof@wobbly.earth>
ENV POSTGRES_MAJOR 18
RUN mkdir /geo_outdb
#RUN apk add --no-cache postgresql$POSTGRES_MAJOR-plpython3
#COPY ./11_geoslurp_extensions.sql /docker-entrypoint-initdb.d/11_geoslurp_extensions.sql
COPY ./12_geoslurp_roles.sql /docker-entrypoint-initdb.d/12_geoslurp_roles.sql
COPY ./13_geoslurp_admin_schema.sql /docker-entrypoint-initdb.d/13_geoslurp_admin_schema.sql

