use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::common::{
    Database, Environment, EnvironmentManager, RenewableRwTransaction, RwTransaction,
};
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::InternalDbError;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use crossbeam::channel::{Receiver, Select};
use dozer_types::types::Schema;
use libc::size_t;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const CHECKPOINT_DB_NAME: &str = "__CHECKPOINT_META";

pub(crate) struct StorageMetadata {
    pub env: Box<dyn EnvironmentManager>,
    pub tx: Box<dyn RenewableRwTransaction>,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(
        env: Box<dyn EnvironmentManager>,
        tx: Box<dyn RenewableRwTransaction>,
        meta_db: Database,
    ) -> Self {
        Self { env, tx, meta_db }
    }
}

pub(crate) fn init_component<F>(
    node_handle: &NodeHandle,
    base_path: PathBuf,
    stateful: bool,
    shared: bool,
    mut init_f: F,
) -> Result<Option<StorageMetadata>, ExecutionError>
where
    F: FnMut(Option<&mut dyn Environment>) -> Result<(), ExecutionError>,
{
    match stateful {
        false => {
            let _ = init_f(None)?;
            Ok(None)
        }
        true => {
            let mut env = LmdbEnvironmentManager::create(base_path, node_handle.as_str())?;
            let db = env.open_database(CHECKPOINT_DB_NAME, false)?;
            init_f(Some(env.as_environment()))?;
            let tx = env.create_txn(shared)?;
            Ok(Some(StorageMetadata::new(env, tx, db)))
        }
    }
}
#[inline]
pub(crate) fn init_select(receivers: &Vec<Receiver<ExecutorOperation>>) -> Select {
    let mut sel = Select::new();
    for r in receivers {
        sel.recv(r);
    }
    sel
}

pub(crate) fn requires_schema_update(
    new: Schema,
    port_handle: &PortHandle,
    input_schemas: &mut HashMap<PortHandle, Schema>,
    input_ports: Vec<PortHandle>,
) -> bool {
    input_schemas.insert(*port_handle, new);
    let count = input_ports
        .iter()
        .filter(|e| !input_schemas.contains_key(*e))
        .count();
    count == 0
}
