use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_utils::{process::run_docker_compose, Cleanup};
use tempdir::TempDir;

use crate::test_suite::DataReadyConnectorTest;

use super::sql::create_table_with_all_supported_data_types;

pub struct PostgresConnectorTest {
    connector: PostgresConnector,
    _cleanup: Cleanup,
    _temp_dir: TempDir,
}

impl DataReadyConnectorTest for PostgresConnectorTest {
    type Connector = PostgresConnector;

    fn new() -> Self {
        let host = "localhost";
        let port = 5432;
        let user = "postgres";
        let password = "postgres";
        let dbname = "dozer-test";

        let temp_dir = TempDir::new("postgres").expect("Failed to create temp dir");
        let docker_compose_path = temp_dir.path().join("docker-compose.yaml");
        std::fs::write(&docker_compose_path, DOCKER_COMPOSE_YAML)
            .expect("Failed to write docker compose file");
        let cleanup =
            run_docker_compose(&docker_compose_path, "dozer-wait-for-connections-healthy");

        let mut config = tokio_postgres::Config::default();
        config
            .host(host)
            .port(port)
            .user(user)
            .password(password)
            .dbname(dbname);

        let mut client = postgres::Config::from(config.clone())
            .connect(postgres::NoTls)
            .unwrap();
        client
            .batch_execute(&create_table_with_all_supported_data_types("test_table"))
            .unwrap();

        let connector = PostgresConnector::new(PostgresConfig {
            name: "postgres_connector_test".to_string(),
            config,
        });
        Self {
            connector,
            _cleanup: cleanup,
            _temp_dir: temp_dir,
        }
    }

    fn connector(&self) -> &Self::Connector {
        &self.connector
    }
}

const DOCKER_COMPOSE_YAML: &str = r#"version: '2.4'
services:
  postgres:
    container_name: postgres
    image: debezium/postgres:13
    ports:
    - target: 5432
      published: 5432
    environment:
    - POSTGRES_DB=dozer-test
    - POSTGRES_USER=postgres
    - POSTGRES_PASSWORD=postgres
    - ALLOW_IP_RANGE=0.0.0.0/0
    healthcheck:
      test:
      - CMD-SHELL
      - pg_isready -U postgres -h 0.0.0.0 -d dozer-test
      interval: 5s
      timeout: 5s
      retries: 5
  dozer-wait-for-connections-healthy:
    image: alpine
    command: echo 'All connections are healthy'
    depends_on:
      postgres:
        condition: service_healthy
"#;
