// use crate::connectors::postgres::connection::helper::{connect, map_connection_config};
// use crate::connectors::{get_connector, TableInfo};
// use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
// use crate::test_util::load_config;
// use dozer_types::models::ingestion_types::KafkaConfig;
// use dozer_types::models::app_config::Config;
// use dozer_types::models::connection::ConnectionConfig;
//
// use dozer_types::serde::{Deserialize, Serialize};
// use dozer_types::serde_yaml;
// use postgres::Client;
// use reqwest::header::{ACCEPT, CONTENT_TYPE};
// use std::thread;
// use std::time::Duration;
//
// #[derive(Debug, Deserialize, Serialize)]
// #[serde(crate = "dozer_types::serde")]
// pub struct DebeziumTestConfig {
//     pub config: Config,
//     pub debezium: DebeziumConnectorConfig,
// }
//
// #[derive(Debug, Deserialize, Serialize)]
// #[serde(crate = "dozer_types::serde")]
// pub struct DebeziumConnectorConfig {
//     pub postgres_source_authentication: ConnectionConfig,
//     pub connector_url: String,
// }
//
// pub fn get_debezium_config(file_name: &str) -> DebeziumTestConfig {
//     let config = serde_yaml::from_str::<Config>(load_config(file_name)).unwrap();
//
//     let debezium =
//         serde_yaml::from_str::<DebeziumConnectorConfig>(load_config("test.debezium.pg.yaml"))
//             .unwrap();
//
//     DebeziumTestConfig { config, debezium }
// }
//
// pub fn get_client_and_create_table(table_name: &str, auth: &ConnectionConfig) -> Client {
//     let postgres_config = map_connection_config(auth).unwrap();
//     let mut client = connect(postgres_config).unwrap();
//
//     client
//         .query(&format!("DROP TABLE IF EXISTS {}", &table_name), &[])
//         .unwrap();
//
//     client
//         .query(
//             &format!(
//                 "CREATE TABLE {}
// (
//     id          SERIAL
//         PRIMARY KEY,
//     name        VARCHAR(255) NOT NULL,
//     description VARCHAR(512),
//     weight      DOUBLE PRECISION
// );",
//                 &table_name
//             ),
//             &[],
//         )
//         .unwrap();
//
//     client
// }
//
// pub fn get_iterator_and_client(table_name: String) -> (IngestionIterator, Client) {
//     let config = get_debezium_config("test.debezium.yaml");
//
//     let client =
//         get_client_and_create_table(&table_name, &config.debezium.postgres_source_authentication);
//
//     let content = load_config("test.register-postgres.json");
//
//     let connector_client = reqwest::blocking::Client::new();
//     connector_client
//         .post(&config.debezium.connector_url)
//         .body(content)
//         .header(CONTENT_TYPE, "application/json")
//         .header(ACCEPT, "application/json")
//         .send()
//         .unwrap()
//         .text()
//         .unwrap();
//
//     let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
//
//     thread::spawn(move || {
//         let tables: Vec<TableInfo> = vec![TableInfo {
//             name: table_name.clone(),
//             table_name: table_name.clone(),
//             id: 0,
//             columns: None,
//         }];
//
//         let mut connection = config.config.connections.get(0).unwrap().clone();
//         if let Some(ConnectionConfig::Kafka(KafkaConfig {
//             broker,
//             schema_registry_url,
//         })) = connection.config
//         {
//             connection.config = Some(ConnectionConfig::Kafka(KafkaConfig {
//                 broker,
//                 schema_registry_url,
//             }));
//         };
//
//         if table_name != "products_test" {
//             thread::sleep(Duration::from_secs(1));
//         }
//
//         let connector = get_connector(connection).unwrap();
//         let _ = connector.start(None, &ingestor, tables);
//     });
//
//     (iterator, client)
// }
