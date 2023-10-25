use dozer_ingestion_connector::dozer_types::models::connection::ConnectionConfig;
use postgres_types::PgLsn;
use std::error::Error;
use std::str::FromStr;
use tokio_postgres::{error::DbError, Error as PostgresError, SimpleQueryMessage};

use crate::connection::helper::map_connection_config;
use crate::replication_slot_helper::ReplicationSlotHelper;
use crate::tests::client::TestPostgresClient;

use super::connection::client::Client;

pub async fn create_slot(client_mut: &mut Client, slot_name: &str) -> PgLsn {
    client_mut
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await
        .unwrap();

    let created_lsn = ReplicationSlotHelper::create_replication_slot(client_mut, slot_name)
        .await
        .unwrap()
        .unwrap();
    client_mut.simple_query("COMMIT;").await.unwrap();

    PgLsn::from_str(&created_lsn).unwrap()
}

pub async fn retry_drop_active_slot(
    e: PostgresError,
    client_mut: &mut Client,
    slot_name: &str,
) -> Result<Vec<SimpleQueryMessage>, PostgresError> {
    match e.source() {
        None => Err(e),
        Some(err) => match err.downcast_ref::<DbError>() {
            Some(db_error) if db_error.code().code().eq("55006") => {
                let err = db_error.to_string();
                let parts = err.rsplit_once(' ').unwrap();

                client_mut
                    .simple_query(format!("select pg_terminate_backend('{}');", parts.1).as_ref())
                    .await
                    .unwrap();

                ReplicationSlotHelper::drop_replication_slot(client_mut, slot_name).await
            }
            _ => Err(e),
        },
    }
}

pub async fn load_test_connection_config() -> ConnectionConfig {
    let config = dozer_ingestion_connector::test_util::load_test_connection_config();
    let postgres_config = map_connection_config(&config).unwrap();
    // We're going to drop `dozer_test` so connect to another database.
    let mut connect_config = postgres_config.clone();
    connect_config.dbname("postgres");
    let mut client = TestPostgresClient::new_with_postgres_config(connect_config).await;
    client
        .execute_query(&format!(
            "DROP DATABASE IF EXISTS {}",
            postgres_config.get_dbname().unwrap()
        ))
        .await;
    client
        .execute_query(&format!(
            "CREATE DATABASE {}",
            postgres_config.get_dbname().unwrap()
        ))
        .await;
    config
}
