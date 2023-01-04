use crate::connectors::postgres::tests::client::TestPostgresClient;
use crate::test_util::load_config;
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::app_config::Config;

use crate::connectors::postgres::test_utils::get_iterator;
use dozer_types::serde_yaml;
use dozer_types::types::{Field, Operation};
use rand::Rng;

#[ignore]
#[test]
// fn connector_e2e_connect_postgres_stream() {
fn connector_disabled_test_e2e_connect_postgres_stream() {
    let config = serde_yaml::from_str::<Config>(load_config("test.postgres.yaml")).unwrap();
    let connection = config.connections.get(0).unwrap().clone();
    let mut client =
        TestPostgresClient::new(&connection.authentication.to_owned().unwrap_or_default());

    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    client.create_simple_table("public", &table_name);

    let iterator = get_iterator(connection, table_name.clone());

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
