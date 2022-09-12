use connectors::connector::Connector;
use connectors::postgres::connector::{PostgresConfig, PostgresConnector};

mod connectors;
mod storage_client;
mod ingestion_server;
#[tokio::main]
async fn main() {
    ingestion_server::get_server().await.unwrap();

    // Use a normal connection till snapshot is created
    let client = storage_client::initialize().await;

    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: None,
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    };
    let mut connector = PostgresConnector::new(postgres_config, client);

    connector.initialize().await;

    // For testing purposes
    connector.drop_replication_slot().await;

    connector.start().await;
}
