use crate::errors::ConnectorError::UnexpectedQueryMessageError;
use crate::errors::PostgresConnectorError::FetchReplicationSlotError;
use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::debug;
use postgres::Client;
use std::cell::RefCell;
use std::sync::Arc;
use tokio_postgres::{Error, SimpleQueryMessage};

pub struct ReplicationSlotHelper {}

impl ReplicationSlotHelper {
    pub fn drop_replication_slot(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<Vec<SimpleQueryMessage>, Error> {
        let res = client
            .borrow_mut()
            .simple_query(format!("select pg_drop_replication_slot('{slot_name}');").as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot_name),
            Err(_) => debug!("failed to drop replication slot..."),
        };

        res
    }

    pub fn create_replication_slot(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<Option<String>, ConnectorError> {
        let create_replication_slot_query =
            format!(r#"CREATE_REPLICATION_SLOT {slot_name:?} LOGICAL "pgoutput" USE_SNAPSHOT"#);

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)
            .map_err(|e| {
                debug!("failed to create replication slot {}", slot_name);
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CreateSlotError(
                    slot_name.to_string(),
                    e,
                ))
            })?;

        if let SimpleQueryMessage::Row(row) = &slot_query_row[0] {
            Ok(row.get("consistent_point").map(|lsn| lsn.to_string()))
        } else {
            Err(UnexpectedQueryMessageError)
        }
    }

    pub fn replication_slot_exists(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<bool, PostgresConnectorError> {
        let replication_slot_info_query =
            format!(r#"SELECT * FROM pg_replication_slots where slot_name = '{slot_name}';"#);

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&replication_slot_info_query)
            .map_err(FetchReplicationSlotError)?;

        Ok(matches!(
            slot_query_row.get(0),
            Some(SimpleQueryMessage::Row(_))
        ))
    }
}
