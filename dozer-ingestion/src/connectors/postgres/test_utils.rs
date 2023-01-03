use crate::connectors::postgres::tests::client::TestPostgresClient;
use std::sync::Arc;
use std::thread;

use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use crate::test_util::load_config;
use dozer_types::models::connection::{Authentication, Connection};
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;

pub fn get_client() -> TestPostgresClient {
    let config =
        serde_yaml::from_str::<Authentication>(load_config("test.postgres.auth.yaml")).unwrap();

    TestPostgresClient::new(&config)
}

pub fn get_iterator(config: Connection, table_name: String) -> Arc<RwLock<IngestionIterator>> {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: table_name.clone(),
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(config).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start(None).unwrap();
    });

    iterator
}
