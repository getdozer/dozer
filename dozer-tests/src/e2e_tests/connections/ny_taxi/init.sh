#!/bin/bash

curl --create-dirs -o data/trips/fhvhv_tripdata_2022-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
chmod -R a+rx data
