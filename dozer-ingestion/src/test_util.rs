use std::ops::Deref;
use std::panic;
use std::path::PathBuf;

use crate::connectors::postgres::tests::client::TestPostgresClient;
use dozer_types::constants::DEFAULT_CONFIG_PATH;
use dozer_types::models::app_config::Config;
use dozer_types::models::connection::ConnectionConfig;
use futures::Future;

async fn warm_up(app_config: &Config) {
    let connection = app_config.connections.get(0).unwrap();
    if let Some(ConnectionConfig::Postgres(connection_config)) = connection.config.clone() {
        let mut config = tokio_postgres::Config::new();
        config
            .user(&connection_config.user)
            .host(&connection_config.host)
            .password(&connection_config.password)
            .port(connection_config.port as u16);

        let client = TestPostgresClient::new_with_postgres_config(config).await;
        client
            .execute_query(&format!(
                "DROP DATABASE IF EXISTS {}",
                connection_config.database
            ))
            .await;
        client
            .execute_query(&format!("CREATE DATABASE {}", connection_config.database))
            .await;
    }
}

pub async fn run_connector_test<F: Future, T: (FnOnce(Config) -> F) + panic::UnwindSafe>(
    db_type: &str,
    test: T,
) {
    let dozer_config_path =
        PathBuf::from(format!("src/tests/cases/{db_type}/{DEFAULT_CONFIG_PATH}"));

    let dozer_config = std::fs::read_to_string(dozer_config_path).unwrap();
    let dozer_config = dozer_types::serde_yaml::from_str::<Config>(&dozer_config).unwrap();

    warm_up(&dozer_config).await;

    test(dozer_config).await;
}

pub fn get_config(app_config: Config) -> tokio_postgres::Config {
    if let Some(ConnectionConfig::Postgres(connection)) =
        &app_config.connections.get(0).unwrap().config
    {
        let mut config = tokio_postgres::Config::new();
        config
            .dbname(&connection.database)
            .user(&connection.user)
            .host(&connection.host)
            .password(&connection.password)
            .port(connection.port as u16)
            .deref()
            .clone()
    } else {
        panic!("Postgres config was expected")
    }
}
