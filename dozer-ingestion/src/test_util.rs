use dozer_orchestrator::cli::load_config;
use std::path::PathBuf;
use std::{panic};

use crate::connectors::postgres::tests::client::TestPostgresClient;
use dozer_types::models::app_config::Config;
use dozer_types::models::connection::ConnectionConfig;

fn warm_up(app_config: &Config) {
    let connection = app_config.connections.get(0).unwrap();
    if let Some(ConnectionConfig::Postgres(connection_config)) = connection.config.clone() {
        let mut config = tokio_postgres::Config::new();
        config
            .user(&connection_config.user)
            .host(&connection_config.host)
            .password(&connection_config.password)
            .port(connection_config.port as u16);

        let mut client = TestPostgresClient::new_with_postgres_config(config);

        client.execute_query(&format!(
            "DROP DATABASE IF EXISTS {}",
            connection_config.database
        ));
        client.execute_query(&format!("CREATE DATABASE {}", connection_config.database));
    }
}

pub fn run_connector_test<T: FnOnce(Config) + panic::UnwindSafe>(db_type: &str, test: T) {
    let dozer_config_path = PathBuf::from(format!("src/tests/cases/{db_type}/dozer-config.yaml"));

    let dozer_config = load_config(dozer_config_path.to_str().unwrap().to_string())
        .unwrap_or_else(|_e| panic!("Cannot read file"));

    warm_up(&dozer_config);
    let result = panic::catch_unwind(|| {
        test(dozer_config);
    });

    assert!(result.is_ok())
}
