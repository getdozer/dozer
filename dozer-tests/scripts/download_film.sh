#!/bin/bash


echo "Downloading Test Files"

function download_glink() {
  wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=$1' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$1" -O $2 && rm -rf /tmp/cookies.txt
}
rm -rf target/debug/pagilla-data
mkdir -p target/debug/pagilla-data

# https://medium.com/@acpanjan/download-google-drive-files-using-wget-3c2c025a8b99
download_glink "1hkDgQUOC-sepc5xDdNEk0vI6jfg46X5b" "target/debug/pagilla-data/films.tar.gz"

cd target/debug/pagilla-data/ && tar -xzvf films.tar.gz 
rm films.tar.gz 

