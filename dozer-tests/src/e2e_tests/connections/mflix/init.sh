#!/bin/bash

set -e
                      
mongorestore --archive=/data/sampledata.archive --nsInclude 'sample_mflix.*'
mongosh sample_mflix --eval 'db.runCommand({collMod: "movies", changeStreamPreAndPostImages: { enabled: true }})'

# Shutdown the server manually, because we need to wait a bit after shutdown
# and the official entrypoint script does wait after it tries to shutdown
# Ignore failure, because we will get a forced disconnect here
mongosh admin --eval 'db.shutdownServer()' || true

sleep 1
