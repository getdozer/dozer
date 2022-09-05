use connectors::connector::Connector;
use connectors::postgres_connector::PostgresConnector;

mod connectors;
mod storage_client;

#[tokio::main]
async fn main() {
    let mut client = storage_client::initialize().await;
    let mut connector = PostgresConnector::new("test_c".to_string(), None, client);

    let str = "host=127.0.0.1 port=5432 user=postgres dbname=pagila replication=database";

    connector.initialize(str.to_string()).await;
    // For testing purposes
    connector.drop_replication_slot().await;

    connector.start().await;
}
