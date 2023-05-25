set -e

# Check if drill is installed
hash drill 2>/dev/null || { echo >&2 "drill (cargo install drill) is needed for load testing grpc service.  Aborting."; exit 1; }

# Start dozer
cargo build -p dozer-cli --release
./target/release/dozer &
DOZER_PID=$!
echo Dozer server pid $DOZER_PID

# Wait for dozer start
sleep 3
echo "\n"

# Run drill
drill --stats --benchmark ./dozer-tests/scripts/load_test_rest/plan.yml

# Stop dozer
kill -s INT $DOZER_PID
