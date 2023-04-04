set -e

curl -sLO https://github.com/protocolbuffers/protobuf/releases/download/v22.2/protoc-22.2-linux-aarch_64.zip
apt install -y unzip
unzip protoc-22.2-linux-aarch_64.zip -d /usr/local
