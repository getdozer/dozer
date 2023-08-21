#!/bin/bash

set -m -e

exec python3 /usr/local/bin/docker-entrypoint.py "$@" &

until mongosh mongodb://localhost/admin --eval \
    'rs.initiate({ "_id": "rs0", "members": [ {"_id": 0, "host": "localhost:27017"} ]})' > /dev/null
do
    sleep 1
done

fg
