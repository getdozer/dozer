use crate::connectors::connector::TableInfo;
use crate::connectors::postgres::connector::ReplicationSlotInfo;

use dozer_types::errors::connector::ConnectorError;
use dozer_types::errors::connector::ConnectorError::InvalidQueryError;
use dozer_types::errors::connector::PostgresConnectorError;

use dozer_types::log::warn;
use postgres::Client;
use postgres_types::PgLsn;
use std::borrow::BorrowMut;
use std::collections::HashMap;

pub fn validate_connection(
    config: tokio_postgres::Config,
    tables: Option<Vec<TableInfo>>,
    replication_info: Option<ReplicationSlotInfo>,
) -> Result<(), ConnectorError> {
    let mut client = super::helper::connect(config)?;

    validate_details(client.borrow_mut())?;
    validate_user(client.borrow_mut())?;

    if let Some(tables_info) = tables {
        validate_tables(client.borrow_mut(), tables_info)?;
    }

    validate_wal_level(client.borrow_mut())?;

    if let Some(replication_details) = replication_info {
        validate_slot(client.borrow_mut(), replication_details)?;
    } else {
        validate_limit_of_replications(client.borrow_mut())?;
    }

    Ok(())
}

fn validate_details(client: &mut Client) -> Result<(), ConnectorError> {
    client
        .simple_query("SELECT version()")
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    Ok(())
}

fn validate_user(client: &mut Client) -> Result<(), ConnectorError> {
    client
        .query_one(
            "SELECT * FROM pg_user WHERE usename = \"current_user\"() AND userepl = true",
            &[],
        )
        .map_err(|_e| PostgresConnectorError::ReplicationIsNotAvailableForUserError())?;

    Ok(())
}

fn validate_wal_level(client: &mut Client) -> Result<(), ConnectorError> {
    let result = client
        .query_one("SHOW wal_level", &[])
        .map_err(|_e| PostgresConnectorError::WALLevelIsNotCorrect())?;

    let wal_level: Result<String, _> = result.try_get(0);

    match wal_level {
        Ok(level) => {
            if level == "logical" {
                Ok(())
            } else {
                Err(ConnectorError::PostgresConnectorError(
                    PostgresConnectorError::WALLevelIsNotCorrect(),
                ))
            }
        }
        Err(e) => Err(ConnectorError::InternalError(Box::new(e))),
    }
}

fn validate_tables(client: &mut Client, table_info: Vec<TableInfo>) -> Result<(), ConnectorError> {
    let mut tables_names: HashMap<String, bool> = HashMap::new();
    table_info.iter().for_each(|t| {
        tables_names.insert(t.name.clone(), true);
    });

    let table_name_keys: Vec<String> = tables_names.keys().cloned().collect();
    let result = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ANY($1)",
            &[&table_name_keys],
        )
        .map_err(|_e| PostgresConnectorError::TableError(table_name_keys))?;

    for r in result.iter() {
        let table_name: String = r.try_get(0).map_err(|_e| InvalidQueryError)?;
        tables_names.remove(&table_name);
    }

    if !tables_names.is_empty() {
        let table_name_keys = tables_names.keys().cloned().collect();
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::TableError(table_name_keys),
        ))
    } else {
        Ok(())
    }
}

fn validate_slot(
    client: &mut Client,
    replication_info: ReplicationSlotInfo,
) -> Result<(), ConnectorError> {
    let result = client
        .query_one(
            "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&replication_info.name],
        )
        .map_err(|_e| PostgresConnectorError::SlotNotExistError(replication_info.name.clone()))?;

    warn!("---------------");
    let is_already_running: bool = result.try_get(0).map_err(|_e| InvalidQueryError)?;
    if is_already_running {
        return Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::SlotIsInUseError(replication_info.name.clone()),
        ));
    }

    let flush_lsn: PgLsn = result.try_get(1).map_err(|_e| InvalidQueryError)?;

    if flush_lsn.gt(&replication_info.start_lsn) {
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::StartLsnIsBeforeLastFlushedLsnError(
                flush_lsn.to_string(),
                replication_info.start_lsn.to_string(),
            ),
        ))
    } else {
        Ok(())
    }
}

fn validate_limit_of_replications(client: &mut Client) -> Result<(), ConnectorError> {
    let slots_limit_result = client
        .query_one("SELECT version()", &[])
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    let slots_limit: i16 = slots_limit_result
        .try_get(0)
        .map_err(|_e| InvalidQueryError)?;

    let used_slots_result = client
        .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    let used_slots: i16 = used_slots_result
        .try_get(0)
        .map_err(|_e| InvalidQueryError)?;

    if used_slots == slots_limit {
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::NoAvailableSlotsError,
        ))
    } else {
        Ok(())
    }
}
