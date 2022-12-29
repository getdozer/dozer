#!/bin/sh

set -eu pipefail

echo "Waiting for stocks cache"
while [ ! -f /mnt/dozer/cache/stocks/data.mdb ]; do
    printf '.'
    sleep 5
done

echo "Cache is ready"

exit 0