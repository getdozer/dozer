#!/bin/bash

set -e

# https://github.com/devrimgunduz/pagila
mkdir -p data

curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-data.sql --output ./data/pagila-data.sql
curl https://raw.githubusercontent.com/devrimgunduz/pagila/726c724df9f86406577c47790d6f8e6f2be06186/pagila-schema.sql --output ./data/pagila-schema.sql
cat ./data/pagila-schema.sql > ./data/init.sql
cat ./data/pagila-data.sql >> ./data/init.sql

rm -f ./data/pagila-schema.sql
rm -f ./data/pagila-data.sql