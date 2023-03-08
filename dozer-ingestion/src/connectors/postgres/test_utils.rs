use crate::connectors::postgres::tests::client::TestPostgresClient;
use postgres::Client;
use postgres_types::PgLsn;
use std::cell::RefCell;
use std::error::Error;
use std::thread;

use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_types::models::app_config::Config;
use dozer_types::models::connection::Connection;
use postgres::error::DbError;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::{Error as PostgresError, SimpleQueryMessage};

pub fn get_client(app_config: Config) -> TestPostgresClient {
    let config = app_config
        .connections
        .get(0)
        .unwrap()
        .config
        .as_ref()
        .unwrap();

    TestPostgresClient::new(config)
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
        connector.start(None, &ingestor, tables).unwrap();
    });

    iterator
}

pub fn create_slot(client_ref: Arc<RefCell<Client>>, slot_name: &str) -> PgLsn {
    client_ref
        .borrow_mut()
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .unwrap();

    let created_lsn = ReplicationSlotHelper::create_replication_slot(client_ref.clone(), slot_name)
        .unwrap()
        .unwrap();
    client_ref.borrow_mut().simple_query("COMMIT;").unwrap();

    PgLsn::from_str(&created_lsn).unwrap()
}

pub fn retry_drop_active_slot(
    e: PostgresError,
    client_ref: Arc<RefCell<Client>>,
    slot_name: &str,
) -> Result<Vec<SimpleQueryMessage>, PostgresError> {
    match e.source() {
        None => Err(e),
        Some(err) => match err.downcast_ref::<DbError>() {
            Some(db_error) if db_error.code().code().eq("55006") => {
                let err = db_error.to_string();
                let parts = err.rsplit_once(' ').unwrap();

                client_ref
                    .borrow_mut()
                    .simple_query(format!("select pg_terminate_backend('{}');", parts.1).as_ref())
                    .unwrap();

                ReplicationSlotHelper::drop_replication_slot(client_ref, slot_name)
            }
            _ => Err(e),
        },
    }
}
