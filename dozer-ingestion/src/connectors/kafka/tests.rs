// use crate::connectors::kafka::connector::KafkaConnector;
// use crate::connectors::kafka::test_utils::{
//     get_client_and_create_table, get_debezium_config, get_iterator_and_client,
// };
// use crate::connectors::{Connector, TableInfo};
// use dozer_types::models::connection::ConnectionConfig;
// use dozer_types::{ingestion_types::KafkaConfig, rust_decimal::Decimal, types::Operation};
// use postgres::Client;
// use std::fmt::Write;
// use std::thread::sleep;
//
// use rand::Rng;
// use std::time::Duration;
//
// pub struct KafkaPostgres {
//     client: Client,
//     table_name: String,
// }
//
// impl KafkaPostgres {
//     pub fn insert_rows(&mut self, count: u64) {
//         let mut buf = String::new();
//         for i in 0..count {
//             if i > 0 {
//                 buf.write_str(",").unwrap();
//             }
//             buf.write_fmt(format_args!(
//                 "(\'Product {}\',\'Product {} description\',{})",
//                 i,
//                 i,
//                 Decimal::new((i * 41) as i64, 2)
//             ))
//             .unwrap();
//         }
//
//         let query = format!(
//             "insert into {}(name, description, weight) values {}",
//             self.table_name, buf,
//         );
//
//         self.client.query(&query, &[]).unwrap();
//     }
//
//     pub fn update_rows(&mut self) {
//         self.client
//             .query(&format!("UPDATE {} SET weight = 5", self.table_name), &[])
//             .unwrap();
//     }
//
//     pub fn delete_rows(&mut self) {
//         self.client
//             .query(
//                 &format!("DELETE FROM {} WHERE weight = 5", self.table_name),
//                 &[],
//             )
//             .unwrap();
//     }
//
//     pub fn drop_table(&mut self) {
//         self.client
//             .query(&format!("DROP TABLE {}", self.table_name), &[])
//             .unwrap();
//     }
// }
//
// #[ignore]
// #[test]
// // fn connector_e2e_connect_debezium_and_use_kafka_stream() {
// fn connector_disabled_test_e2e_connect_debezium_and_use_kafka_stream() {
//     let mut rng = rand::thread_rng();
//     let table_name = format!("products_test_{}", rng.gen::<u32>());
//     let (mut iterator, client) = get_iterator_and_client(table_name.clone());
//
//     let mut pg_client = KafkaPostgres { client, table_name };
//
//     pg_client.insert_rows(10);
//     pg_client.update_rows();
//     pg_client.delete_rows();
//
//     let mut i = 0;
//     while i < 30 {
//         let op = iterator.next();
//
//         if let Some((_, op)) = op {
//             i += 1;
//             match op {
//                 Operation::Insert { .. } => {
//                     if i > 10 {
//                         panic!("Unexpected operation");
//                     }
//                 }
//                 Operation::Delete { .. } => {
//                     if i < 21 {
//                         panic!("Unexpected operation");
//                     }
//                 }
//                 Operation::Update { .. } => {
//                     if !(11..=20).contains(&i) {
//                         panic!("Unexpected operation");
//                     }
//                 }
//                 Operation::SnapshottingDone {} => (),
//             }
//         }
//     }
//
//     pg_client.drop_table();
//
//     assert_eq!(i, 30);
// }
//
// #[ignore]
// #[test]
// // fn connector_e2e_connect_debezium_json_and_get_schema() {
// fn connector_disabled_test_e2e_connect_debezium_json_and_get_schema() {
//     let mut rng = rand::thread_rng();
//     let table_name = format!("products_test_{}", rng.gen::<u32>());
//     let topic = format!("dbserver1.public.{table_name}");
//     let config = get_debezium_config("test.debezium.yaml");
//
//     let client =
//         get_client_and_create_table(&table_name, &config.debezium.postgres_source_authentication);
//
//     let mut pg_client = KafkaPostgres { client, table_name };
//     pg_client.insert_rows(1);
//
//     let broker = if let ConnectionConfig::Kafka(KafkaConfig { broker, .. }) = config
//         .config
//         .connections
//         .get(0)
//         .unwrap()
//         .clone()
//         .config
//         .unwrap()
//     {
//         broker
//     } else {
//         todo!()
//     };
//     let connector = KafkaConnector::new(
//         1,
//         KafkaConfig {
//             broker,
//             schema_registry_url: None,
//         },
//     );
//
//     sleep(Duration::from_secs(2));
//     let schemas = connector
//         .get_schemas(Some(vec![TableInfo {
//             name: topic.clone(),
//             table_name: topic.clone(),
//             id: 0,
//             columns: None,
//         }]))
//         .unwrap();
//
//     pg_client.drop_table();
//
//     assert_eq!(topic, schemas.get(0).unwrap().name);
//     assert_eq!(4, schemas.get(0).unwrap().schema.fields.len());
// }
//
// #[ignore]
// #[test]
// // fn connector_e2e_connect_debezium_avro_and_get_schema() {
// fn connector_disabled_test_e2e_connect_debezium_avro_and_get_schema() {
//     let mut rng = rand::thread_rng();
//     let table_name = format!("products_test_{}", rng.gen::<u32>());
//     let topic = format!("dbserver1.public.{table_name}");
//     let config = get_debezium_config("test.debezium-with-schema-registry.yaml");
//
//     let client =
//         get_client_and_create_table(&table_name, &config.debezium.postgres_source_authentication);
//
//     let mut pg_client = KafkaPostgres { client, table_name };
//     pg_client.insert_rows(1);
//
//     let (broker, schema_registry_url) =
//         if let dozer_types::models::connection::ConnectionConfig::Kafka(KafkaConfig {
//             broker,
//             schema_registry_url,
//             ..
//         }) = config
//             .config
//             .connections
//             .get(0)
//             .unwrap()
//             .clone()
//             .config
//             .unwrap()
//         {
//             (broker, schema_registry_url)
//         } else {
//             todo!()
//         };
//     let connector = KafkaConnector::new(
//         1,
//         KafkaConfig {
//             broker,
//             schema_registry_url,
//         },
//     );
//
//     sleep(Duration::from_secs(1));
//     let schemas = connector
//         .get_schemas(Some(vec![TableInfo {
//             name: topic.clone(),
//             table_name: topic.clone(),
//             id: 0,
//             columns: None,
//         }]))
//         .unwrap();
//
//     pg_client.drop_table();
//
//     assert_eq!(topic, schemas.get(0).unwrap().name);
//     assert_eq!(4, schemas.get(0).unwrap().schema.fields.len());
// }
