use connectors::connector::Connector;
use connectors::postgres_connector::PostgresConnector;

mod connectors;

#[tokio::main]
async fn main() {
    let mut connector = PostgresConnector::new("test_c".to_string(), None);

    let str = "host=127.0.0.1 port=5432 user=postgres dbname=pagila replication=database";

    connector.initialize(str.to_string()).await;
    // FOr testing purposes
    connector.drop_replication_slot().await;
    
    connector.start().await;
}
