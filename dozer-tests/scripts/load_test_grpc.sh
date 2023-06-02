set -e

# Check if ghz is installed
hash ghz 2>/dev/null || { echo >&2 "ghz (https://ghz.sh) is needed for load testing grpc service.  Aborting."; exit 1; }

# Start dozer
cargo build -p dozer-cli --release
./target/release/dozer &
DOZER_PID=$!
echo Dozer server pid $DOZER_PID

# Wait for dozer start
sleep 3
echo "\n"

# Run ghz
HOST=localhost:50051
TOTAL=1000
CONCURRENCY=50
echo "Testing common grpc service with $TOTAL requests and $CONCURRENCY concurrency"
ghz --insecure --proto ./dozer-api/protos/common.proto --call dozer.common.CommonGrpcService.query --total $TOTAL --concurrency $CONCURRENCY --data '{"endpoint":"users"}' $HOST
echo "Testing typed grpc service with $TOTAL requests and $CONCURRENCY concurrency"
ghz --insecure --proto .dozer/generated/users.proto --call dozer.generated.users.Users.query --total $TOTAL --concurrency $CONCURRENCY $HOST

# Stop dozer
kill -s INT $DOZER_PID
