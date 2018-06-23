#!/bin/bash

#import password and location of the postgresql data (don't put in version control)
source .geoenv

#check if the necessary environment variables exist
if [ -z "$GEOSLURPPASS" ]
then
    echo please set the variable GEOSLURPPASS
    exit 1
fi

if [ -z "$GEOSLURPPGDATA" ]
then
    echo please set the variable GEOSLURPPGDATA
    exit 1
fi



envsubst < docker-compose_template.yml > docker-compose.yml

#run docker compose
docker-compose up -d
