use std::path::Path;
use crate::dag::errors::ExecutionError;
use crate::dag::node::NodeHandle;
use crate::storage::common::Database;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;

pub(crate) fn get_checkpoint_metadata(path: &Path, name: &str) -> Result<(NodeHandle, u64), ExecutionError> {


    let env = LmdbEnvironmentManager::create(path, name)




}
