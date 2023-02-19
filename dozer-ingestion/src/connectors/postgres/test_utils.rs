use crate::connectors::postgres::tests::client::TestPostgresClient;
use std::thread;

use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use crate::test_util::load_config;
use dozer_types::models::connection::{Connection, ConnectionConfig};
use dozer_types::serde_yaml;

pub fn get_client() -> TestPostgresClient {
    let config =
        serde_yaml::from_str::<ConnectionConfig>(load_config("test.postgres.auth.yaml")).unwrap();

    TestPostgresClient::new(&config)
}

pub fn get_iterator(config: Connection, table_name: String) -> IngestionIterator {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: table_name.clone(),
            table_name: table_name.clone(),
            id: 0,
            columns: None,
        }];

        let connector = get_connector(config).unwrap();
        connector.start(None, &ingestor, Some(tables)).unwrap();
    });

    iterator
}
