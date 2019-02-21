#!/bin/bash

## This bash script may aid to start a dockerized instance of a postgis/postgresql database which is suitable for use with geoslurp


# ifunction to decrypt sensitive data and source it in the current shell
function sourcePrivate {
  echo  $(dirname $0)
  gpg2 -d $1 > tmp.private
  source tmp.private
  rm tmp.private
}

#import admin user + password as environment variables
sourcePrivate geoslurpcredentials.gpg

#check if the necessary environment variables exist
if [ -z "$GEOSLURPPASS" ]
then
    echo please set the variable GEOSLURPPASS
    exit 1
fi

#check if the necessary environment variables exist
if [ -z "$GEOSLURPADMIN" ]
then
    echo please set the variable GEOSLURPADMIN
    exit 1
fi

if [ -z "$GEOSLURPPGDATA" ]
then
    echo please set the variable GEOSLURPPGDATA
    exit 1
fi


#build the docker image
tag=rrpostgis-11
docker build -t ${tag} .

#run the docker container with the appropriate environment variables and linked volumes
docker run --name postgis --restart always -p 5432:5432 -e POSTGRES_PASSWORD="${GEOSLURPPASS}" -e  POSTGRES_DB=geoslurp -e POSTGRES_USER=${GEOSLURPADMIN} -v ${GEOSLURPPGDATA}:/var/lib/postgresql/data -d ${tag}
