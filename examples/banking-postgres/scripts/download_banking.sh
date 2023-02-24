#!/bin/bash


echo "Downloading Postgres initialization data..."
export LANG=en_US.UTF-8
export LC_ALL=$LANG

function download_glink() {
  wget --quiet --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=$1' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$1" -O $2 && rm -rf /tmp/cookies.txt 
}

download_glink "1dzhDdvJdQ6tHH0h1iVSLQZEDB7Q1ixj3" "./scripts/banking_data.sql"
