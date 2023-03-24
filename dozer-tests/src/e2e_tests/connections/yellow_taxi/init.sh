#!/bin/bash

curl --create-dirs -o data/yellow_trips/yellow_tripdata_2022-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet
curl --create-dirs -o data/zones/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
chmod -R a+rx data
