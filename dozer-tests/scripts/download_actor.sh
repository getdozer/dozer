#!/bin/bash


echo "Downloading Test Files"

function download_glink() {
  wget --quiet --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=$1' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$1" -O $2 && rm -rf /tmp/cookies.txt 
}
rm -rf ../target/debug/actor-data
mkdir -p ../target/debug/actor-data

# https://medium.com/@acpanjan/download-google-drive-files-using-wget-3c2c025a8b99
download_glink "1cdPu4zykKF3E5bfiRQhSHff_qUmmNs_P" "../target/debug/actor-data/actor.tar.gz"

cd ../target/debug/actor-data/ && tar -xzf actor.tar.gz 
rm actor.tar.gz 

