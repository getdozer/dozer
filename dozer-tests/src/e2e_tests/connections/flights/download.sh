#!/bin/bash

mkdir -p tmpdir
cd tmpdir


curl https://edu.postgrespro.com/demo-small-en.zip --output ./demo-small-en.zip
unzip -f demo-small-en.zip
rm -rf ./data
mkdir -p ./data
cp demo-small-en-20170815.sql ./data/init.sql
rm -rf tmpdir


set -e

mkdir -p data

# Uncomment the following lines for a larger data set
# curl https://edu.postgrespro.com/demo-big-en.zip --output ./demo-big-en.zip
# unzip demo-big-en.zip 

curl https://edu.postgrespro.com/demo-small-en.zip --output ./demo-small-en.zip
unzip -f demo-small-en.zip

cat ./demo-small-en-20170815.sql > ./data/init.sql

rm -f ./demo-small-en-20170815.sql
