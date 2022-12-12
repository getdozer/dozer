use crate::connectors::postgres::connection::helper::{connect, map_connection_config};
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use crate::test_util::load_config;
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::models::connection::Authentication;
use dozer_types::models::source::Source;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_yaml;
use postgres::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumTestConfig {
    pub source: Source,
    pub postgres_source_authentication: Authentication,
    pub debezium_connector_url: String,
}

pub fn get_iterator_and_client(
    _prefix: &str,
    table_name: String,
) -> (Arc<RwLock<IngestionIterator>>, Client) {
    let mut config =
        serde_yaml::from_str::<DebeziumTestConfig>(load_config("test.debezium.yaml")).unwrap();

    config.source.table_name = table_name.clone();

    let mut source = config.source;
    let postgres_config = map_connection_config(&config.postgres_source_authentication).unwrap();

    let mut client = connect(postgres_config).unwrap();

    client
        .query(&format!("DROP TABLE IF EXISTS {}", &table_name), &[])
        .unwrap();

    client
        .query(
            &format!(
                "CREATE TABLE {}
(
    id          SERIAL
        PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      DOUBLE PRECISION
);",
                &table_name
            ),
            &[],
        )
        .unwrap();

    let content = load_config("test.register-postgres.json");

    let connector_client = reqwest::blocking::Client::new();
    connector_client
        .post(&config.debezium_connector_url)
        .body(content)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .send()
        .unwrap()
        .text()
        .unwrap();

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: table_name.clone(),
            id: 0,
            columns: None,
        }];
        if let Some(connection) = source.connection.to_owned() {
            if let Some(authentication) = connection.to_owned().authentication {
                if let Authentication::Kafka(kafka_config) = authentication {
                    let mut new_connection = connection;
                    new_connection.authentication = Some(Authentication::Kafka(KafkaConfig {
                        broker: kafka_config.broker,
                        topic: format!("dbserver1.public.{}", table_name),
                    }));
                    source.connection = Some(new_connection);
                }
            }
        }
        if table_name != "products_test" {
            thread::sleep(Duration::from_secs(1));
        }

        let mut connector = get_connector(source.connection.unwrap_or_default()).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    (iterator, client)
}
