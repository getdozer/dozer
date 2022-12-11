use crate::connectors::kafka::connector::KafkaConnector;
use crate::connectors::kafka::test_utils::{
    get_client_and_create_table, get_iterator_and_client, DebeziumTestConfig,
};

use crate::connectors::{Connector, TableInfo};
use crate::test_util::load_config;
use dozer_types::ingestion_types::{IngestionOperation, KafkaConfig};
use dozer_types::models::connection::Authentication::KafkaAuthentication;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::Operation;
use postgres::Client;
use std::fmt::Write;
use std::thread::sleep;

use dozer_types::serde_yaml;
use rand::Rng;
use std::time::{Duration};

pub struct KafkaPostgres {
    client: Client,
    table_name: String,
}

impl KafkaPostgres {
    pub fn insert_rows(&mut self, count: u64) {
        let mut buf = String::new();
        for i in 0..count {
            if i > 0 {
                buf.write_str(",").unwrap();
            }
            buf.write_fmt(format_args!(
                "(\'Product {}\',\'Product {} description\',{})",
                i,
                i,
                Decimal::new((i * 41) as i64, 2)
            ))
            .unwrap();
        }

        let query = format!(
            "insert into {}(name, description, weight) values {}",
            self.table_name, buf,
        );

        self.client.query(&query, &[]).unwrap();
    }

    pub fn update_rows(&mut self) {
        self.client
            .query(&format!("UPDATE {} SET weight = 5", self.table_name), &[])
            .unwrap();
    }

    pub fn delete_rows(&mut self) {
        self.client
            .query(
                &format!("DELETE FROM {} WHERE weight = 5", self.table_name),
                &[],
            )
            .unwrap();
    }

    pub fn drop_table(&mut self) {
        self.client
            .query(&format!("DROP TABLE {}", self.table_name), &[])
            .unwrap();
    }
}

#[ignore]
#[test]
fn connector_e2e_connect_debezium_and_use_kafka_stream() {
    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());
    let (iterator, client) = get_iterator_and_client(table_name.clone());

    let mut pg_client = KafkaPostgres { client, table_name };

    pg_client.insert_rows(10);
    pg_client.update_rows();
    pg_client.delete_rows();

    let mut i = 0;
    while i < 30 {
        let op = iterator.write().next();

        if let Some((_, IngestionOperation::OperationEvent(ev))) = op {
            i += 1;
            match ev.operation {
                Operation::Insert { .. } => {
                    if i > 10 {
                        panic!("Unexpected operation");
                    }
                }
                Operation::Delete { .. } => {
                    if i < 21 {
                        panic!("Unexpected operation");
                    }
                }
                Operation::Update { .. } => {
                    if !(11..=20).contains(&i) {
                        panic!("Unexpected operation");
                    }
                }
            }
        }
    }

    pg_client.drop_table();

    assert_eq!(i, 30);
}

#[ignore]
#[test]
fn connector_e2e_connect_debezium_json_and_get_schema() {
    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());
    let topic = format!("dbserver1.public.{}", table_name);
    let config =
        serde_yaml::from_str::<DebeziumTestConfig>(load_config("test.debezium.yaml")).unwrap();

    let client = get_client_and_create_table(&table_name, &config.postgres_source_authentication);

    let mut pg_client = KafkaPostgres { client, table_name };
    pg_client.insert_rows(1);

    let broker = if let KafkaAuthentication { broker, .. } = config.source.connection.authentication
    {
        broker
    } else {
        todo!()
    };
    let connector = KafkaConnector::new(
        1,
        KafkaConfig {
            broker,
            topic: topic.clone(),
            schema_registry_url: None,
        },
    );

    sleep(Duration::from_secs(2));
    let schemas = connector
        .get_schemas(Some(vec![TableInfo {
            name: topic.clone(),
            id: 0,
            columns: None,
        }]))
        .unwrap();

    pg_client.drop_table();

    assert_eq!(topic, schemas.get(0).unwrap().0);
    assert_eq!(4, schemas.get(0).unwrap().1.fields.len());
}

#[ignore]
#[test]
fn connector_e2e_connect_debezium_avro_and_get_schema() {
    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());
    let topic = format!("dbserver1.public.{}", table_name);
    let config = serde_yaml::from_str::<DebeziumTestConfig>(load_config(
        "test.debezium-with-schema-registry.yaml",
    ))
    .unwrap();

    let client = get_client_and_create_table(&table_name, &config.postgres_source_authentication);

    let mut pg_client = KafkaPostgres { client, table_name };
    pg_client.insert_rows(1);

    let (broker, schema_registry_url) = if let KafkaAuthentication {
        broker,
        schema_registry_url,
        ..
    } = config.source.connection.authentication
    {
        (broker, schema_registry_url)
    } else {
        todo!()
    };
    let connector = KafkaConnector::new(
        1,
        KafkaConfig {
            broker,
            topic: topic.clone(),
            schema_registry_url,
        },
    );

    sleep(Duration::from_secs(1));
    let schemas = connector
        .get_schemas(Some(vec![TableInfo {
            name: topic.clone(),
            id: 0,
            columns: None,
        }]))
        .unwrap();

    pg_client.drop_table();

    assert_eq!(topic, schemas.get(0).unwrap().0);
    assert_eq!(4, schemas.get(0).unwrap().1.fields.len());
}
