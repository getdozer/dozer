use dozer_storage::{
    lmdb::{Cursor, Database, DatabaseFlags, RwTransaction, Transaction, WriteFlags},
    lmdb_storage::LmdbEnvironmentManager,
};
use dozer_types::node::{NodeHandle, OpIdentifier, SourceStates};

use crate::errors::{CacheError, QueryError};

#[derive(Debug, Clone, Copy)]
pub struct CheckpointDatabase(Database);

impl CheckpointDatabase {
    pub fn new(env: &mut LmdbEnvironmentManager) -> Result<Self, CacheError> {
        Ok(Self(env.create_database(
            Some("checkpoint"),
            Some(DatabaseFlags::empty()),
        )?))
    }

    pub fn write(
        &self,
        txn: &mut RwTransaction,
        source_states: &SourceStates,
    ) -> Result<(), CacheError> {
        txn.clear_db(self.0)
            .map_err(|e| CacheError::Internal(Box::new(e)))?;
        for (node_handle, op_identifier) in source_states {
            let key = node_handle.to_bytes();
            let value = op_identifier.to_bytes();
            txn.put(self.0, &key, &value, WriteFlags::empty())
                .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))?;
        }
        Ok(())
    }

    pub fn read<T: Transaction>(&self, txn: &T) -> Result<SourceStates, CacheError> {
        let mut cursor = txn
            .open_ro_cursor(self.0)
            .map_err(|e| CacheError::Internal(Box::new(e)))?;
        let mut result = SourceStates::new();
        for key_value_pair in cursor.iter_start() {
            let (key, value) = key_value_pair.map_err(|e| CacheError::Internal(Box::new(e)))?;
            let node_handle = NodeHandle::from_bytes(key);
            let op_identifier = OpIdentifier::from_bytes(
                value
                    .try_into()
                    .expect("We only write op identifier to this database"),
            );
            result.insert(node_handle, op_identifier);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_checkpoint_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap().0;
        let db = CheckpointDatabase::new(&mut env).unwrap();
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();

        // Write to empty database.
        let mut source_states_1 = SourceStates::new();
        source_states_1.insert(
            NodeHandle::new(Some(1), "1".to_string()),
            OpIdentifier::new(1, 1),
        );
        db.write(txn.txn_mut(), &source_states_1).unwrap();
        assert_eq!(db.read(txn.txn()).unwrap(), source_states_1);

        // Write to existing database.
        let mut source_states_2 = SourceStates::new();
        source_states_2.insert(
            NodeHandle::new(Some(2), "2".to_string()),
            OpIdentifier::new(2, 2),
        );
        db.write(txn.txn_mut(), &source_states_2).unwrap();
        assert_eq!(db.read(txn.txn()).unwrap(), source_states_2);
    }
}
