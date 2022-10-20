#!/bin/sh

mkdir data;

# https://github.com/devrimgunduz/pagila
echo "Downloading Pagilla files...."

curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-data.sql --output ./data/pagila-data.sql
curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-schema.sql --output ./data/pagila-schema.sql

echo "Creating schema and insert data"
echo "CREATE DATABASE pagila;" | docker-compose exec -T postgres psql  -U postgres

cat ./data/pagila-schema.sql | docker-compose exec -T postgres psql -U postgres -d pagila

cat ./data/pagila-data.sql | docker-compose exec -T postgres psql -U postgres -d pagila
