use crate::dag::errors::ExecutionError;
use crate::dag::executor_utils::CHECKPOINT_DB_NAME;
use crate::dag::forwarder::{SCHEMA_IDENTIFIER, SOURCE_ID_IDENTIFIER};
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::Database;
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::{DeserializationError, InvalidRecord};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::path::Path;

pub(crate) struct CheckpointMetadata {
    pub commits: HashMap<NodeHandle, u64>,
    pub schemas: HashMap<PortHandle, Schema>,
}

pub(crate) fn get_checkpoint_metadata(
    path: &Path,
    name: &str,
) -> Result<CheckpointMetadata, ExecutionError> {
    let mut env = LmdbEnvironmentManager::create(path, name)?;
    let db = env.open_database(CHECKPOINT_DB_NAME, false)?;
    let txn = env.create_txn()?;

    let cur = txn.open_cursor(&db)?;
    if !cur.first()? {
        return Err(ExecutionError::InternalDatabaseError(
            StorageError::InvalidRecord,
        ));
    }

    let mut map = HashMap::<NodeHandle, u64>::new();
    let mut schemas: Option<HashMap<PortHandle, Schema>> = None;

    loop {
        let value = cur.read()?.ok_or(ExecutionError::InternalDatabaseError(
            StorageError::InvalidRecord,
        ))?;
        match value.0[0] {
            SOURCE_ID_IDENTIFIER => {
                let handle: NodeHandle = String::from_utf8_lossy(&value.0[1..]).to_string();
                let seq: u64 = u64::from_be_bytes(value.1.try_into().unwrap());
                map.insert(handle, seq);
            }
            SCHEMA_IDENTIFIER => {
                schemas = Some(
                    bincode::deserialize(value.1).map_err(|e| DeserializationError {
                        typ: "HashMap<PortHandle, Schema>".to_string(),
                        reason: Box::new(e),
                    })?,
                )
            }
            _ => {
                return Err(ExecutionError::InternalDatabaseError(
                    StorageError::InvalidRecord,
                ))
            }
        }
        if !cur.next()? {
            break;
        }
    }

    Ok(CheckpointMetadata {
        commits: map,
        schemas: schemas.ok_or(InvalidRecord)?,
    })
}
