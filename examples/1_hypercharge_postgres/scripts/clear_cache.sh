#!/bin/sh

set -eu pipefail

echo "Preparing to clear cache"
if [ -d /mnt/dozer/cache ]; then
  rm -rf /mnt/dozer/cache
  echo "Clearing cache"
fi;

echo "Cache is cleared"

exit 0