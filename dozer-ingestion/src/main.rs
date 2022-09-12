use connectors::connector::Connector;
use connectors::postgres::connector::{PostgresConfig, PostgresConnector};
mod connectors;
use std::sync::Arc;
// mod ingestion_server;
use dozer_storage::storage::{RocksConfig, Storage};
#[tokio::main]
async fn main() {
    // ingestion_server::get_server().await.unwrap();

    let storage_config = RocksConfig {
        path: "./db/embedded".to_string(),
    };
    let storage_client = Arc::new(Storage::new(storage_config));

    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: None,
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    };
    let mut connector = PostgresConnector::new(postgres_config, storage_client);

    connector.initialize().await;

    // For testing purposes
    connector.drop_replication_slot().await;

    connector.start().await;
}
