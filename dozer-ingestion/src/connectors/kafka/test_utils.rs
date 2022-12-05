use crate::connectors::postgres::connection::helper::{connect, map_connection_config};
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_types::models::connection::Authentication;
use dozer_types::models::source::Source;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde::{Deserialize, Serialize};
use postgres::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

#[derive(Debug, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumTestConfig {
    pub source: Source,
    pub postgres_source_authentication: Authentication,
    pub debezium_connector_url: String,
}

pub fn load_config(config_path: String) -> Result<DebeziumTestConfig, serde_yaml::Error> {
    let contents = fs::read_to_string(config_path).unwrap();

    serde_yaml::from_str::<DebeziumTestConfig>(&contents)
}

pub fn get_iterator_and_client(
    prefix: &str,
    table_name: String,
) -> (Arc<RwLock<IngestionIterator>>, Client) {
    let mut config = load_config(format!("{}config/tests/test.debezium.yaml", prefix)).unwrap();

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

    let content = std::fs::read_to_string(format!(
        "{}tests/connectors/debezium/register-postgres.test.json",
        prefix
    ))
    .unwrap();

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

        if let Authentication::KafkaAuthentication { broker, topic: _ } =
            source.connection.authentication
        {
            source.connection.authentication = Authentication::KafkaAuthentication {
                broker,
                topic: format!("dbserver1.public.{}", table_name),
            };
        };

        if table_name != "products_test" {
            thread::sleep(Duration::from_secs(1));
        }

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    (iterator, client)
}
