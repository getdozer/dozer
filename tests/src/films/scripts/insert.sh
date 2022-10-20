#!/bin/sh

echo "Creating schema and insert data"
echo "CREATE DATABASE films;" | psql  -U postgres

cat /var/scripts/data/pagila-schema.sql | psql -U postgres -d films

cat /var/scripts/data/pagila-data.sql | psql -U postgres -d films
