use crate::connectors::postgres::tests::client::TestPostgresClient;
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use crate::test_util::load_config;
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::app_config::Config;
use dozer_types::models::connection::Connection;

use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;
use dozer_types::types::{Field, Operation};
use rand::Rng;
use std::sync::Arc;
use std::thread;

fn get_iterator(config: Connection, table_name: String) -> Arc<RwLock<IngestionIterator>> {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: table_name.clone(),
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(config).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    iterator
}

#[ignore]
#[test]
fn connector_e2e_connect_postgres_stream() {
    let source = serde_yaml::from_str::<Source>(load_config("test.postgres.yaml")).unwrap();
    let mut client = TestPostgresClient::new(
        &source
            .to_owned()
            .connection
            .unwrap_or_default()
            .authentication
            .unwrap_or_default(),
    );

    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    client.create_simple_table("public", &table_name);

    let connetion = source.connection.unwrap_or_default();
    let iterator = get_iterator(connetion, table_name.clone());

    client.insert_rows(&table_name, 10);

    let mut i = 1;
    while i < 10 {
        let op = iterator.write().next();
        if let Some((_, IngestionOperation::OperationEvent(ev))) = op {
            if let Operation::Insert { new } = ev.operation {
                assert_eq!(new.values.get(0).unwrap(), &Field::Int(i));
                i += 1;
            }
        }
    }
    client.insert_rows(&table_name, 10);

    while i < 20 {
        let op = iterator.write().next();

        if let Some((_, IngestionOperation::OperationEvent(ev))) = op {
            if let Operation::Insert { new } = ev.operation {
                assert_eq!(new.values.get(0).unwrap(), &Field::Int(i));
                i += 1;
            }
        }
    }

    client.drop_table("public", &table_name);
    assert_eq!(i, 20);
}
