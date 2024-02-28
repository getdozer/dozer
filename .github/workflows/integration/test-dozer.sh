set -e

# Check if dozer version matches `DOZER_VERSION`
dozer -V | grep "$DOZER_VERSION"
