# Dozer Tests

## Films

Features used:
- Init project from yaml
- Ingestion from postgresql source
- SQL execution

## Run

docker-compose up --build
sh ./scripts/download_and_insert.sh
docker exec -d dozer-orchestrator ./target/debug/dozer-schema
docker exec dozer-orchestrator ./target/debug/dozer-orchestrator run -c ./tests/src/films/dozer-run.yaml
sh ./scripts/run_test.sh
