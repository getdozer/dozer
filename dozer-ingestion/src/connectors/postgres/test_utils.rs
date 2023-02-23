use std::cell::RefCell;
use crate::connectors::postgres::tests::client::TestPostgresClient;
use std::thread;
use postgres::Client;
use postgres_types::PgLsn;

use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use crate::test_util::load_config;
use dozer_types::models::connection::{Connection, ConnectionConfig};
use dozer_types::serde_yaml;
use std::sync::Arc;
use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
use std::str::FromStr;

pub fn get_client() -> TestPostgresClient {
    let config =
        serde_yaml::from_str::<ConnectionConfig>(load_config("test.postgres.auth.yaml")).unwrap();

    TestPostgresClient::new(&config)
}

pub fn get_iterator(config: Connection, table_name: String) -> IngestionIterator {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: table_name.clone(),
            table_name: table_name.clone(),
            id: 0,
            columns: None,
        }];

        let connector = get_connector(config).unwrap();
        connector.start(None, &ingestor, Some(tables)).unwrap();
    });

    iterator
}

pub fn create_slot(client_ref: Arc<RefCell<Client>>, slot_name: &str) -> PgLsn {
    client_ref
        .borrow_mut()
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .unwrap();

    let created_lsn =
        ReplicationSlotHelper::create_replication_slot(client_ref.clone(), slot_name)
            .unwrap()
            .unwrap();
    client_ref.borrow_mut().simple_query("COMMIT;").unwrap();

    PgLsn::from_str(&created_lsn).unwrap()
}
