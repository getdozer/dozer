use crate::dag_metadata::METADATA_DB_NAME;
use crate::errors::ExecutionError;
use crate::executor::ExecutorOperation;
use crate::node::NodeHandle;
use crossbeam::channel::{Receiver, Select};
use dozer_storage::common::Database;
use dozer_storage::lmdb::DatabaseFlags;
use dozer_storage::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions};
use std::path::Path;

pub(crate) struct StorageMetadata {
    pub env: LmdbEnvironmentManager,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(env: LmdbEnvironmentManager, meta_db: Database) -> Self {
        Self { env, meta_db }
    }
}

pub(crate) fn init_component<F>(
    node_handle: &NodeHandle,
    base_path: &Path,
    mut init_f: F,
) -> Result<StorageMetadata, ExecutionError>
where
    F: FnMut(&mut LmdbEnvironmentManager) -> Result<(), ExecutionError>,
{
    let mut env = LmdbEnvironmentManager::create(
        base_path,
        format!("{node_handle}").as_str(),
        LmdbEnvironmentOptions::default(),
    )?;
    let db = env.create_database(Some(METADATA_DB_NAME), Some(DatabaseFlags::empty()))?;
    init_f(&mut env)?;
    Ok(StorageMetadata::new(env, db))
}
#[inline]
pub(crate) fn init_select(receivers: &Vec<Receiver<ExecutorOperation>>) -> Select {
    let mut sel = Select::new();
    for r in receivers {
        sel.recv(r);
    }
    sel
}
