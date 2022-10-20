#!/bin/sh

mkdir data;

# https://github.com/devrimgunduz/pagila
echo "Downloading Pagilla files...."

curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-data.sql --output ./data/films-data.sql
curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-schema.sql --output ./data/films-schema.sql

echo "Creating schema and insert data"
echo "CREATE DATABASE films;" | docker exec -i dozer-tests-films-db psql -U postgres

cat ./data/films-schema.sql | docker exec -i dozer-tests-films-db psql -U postgres -d films

cat ./data/films-data.sql | docker exec -i dozer-tests-films-db psql -U postgres -d films
