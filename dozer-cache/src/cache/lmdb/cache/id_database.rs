use dozer_storage::{
    lmdb::{Database, DatabaseFlags, RwTransaction, Transaction, WriteFlags},
    lmdb_storage::LmdbEnvironmentManager,
};

use super::helper;
use crate::errors::{CacheError, QueryError};

#[derive(Debug, Clone, Copy)]
pub struct IdDatabase(Database);

impl IdDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let flags = if create_if_not_exist {
            Some(DatabaseFlags::empty())
        } else {
            None
        };
        let db = env.create_database(Some("primary_index"), flags)?;
        Ok(Self(db))
    }

    pub fn get_or_generate(
        &self,
        txn: &mut RwTransaction,
        key: Option<&[u8]>,
    ) -> Result<[u8; 8], CacheError> {
        if let Some(key) = key {
            match txn.get(self.0, &key) {
                Ok(id) => Ok(id
                    .try_into()
                    .expect("All values must be u64 ids in this database")),
                Err(dozer_storage::lmdb::Error::NotFound) => self.generate_id(txn, Some(key)),
                Err(e) => Err(CacheError::Query(QueryError::InsertValue(e))),
            }
        } else {
            self.generate_id(txn, None)
        }
    }

    fn generate_id(
        &self,
        txn: &mut RwTransaction,
        key: Option<&[u8]>,
    ) -> Result<[u8; 8], CacheError> {
        let id = helper::lmdb_stat(txn, self.0)
            .map_err(|e| CacheError::Internal(Box::new(e)))?
            .ms_entries as u64;
        let id: [u8; 8] = id.to_be_bytes();

        let key = key.unwrap_or(&id);

        txn.put(self.0, &key, &id, WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))?;

        Ok(id)
    }

    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<[u8; 8], CacheError> {
        txn.get(self.0, &key)
            .map_err(|e| CacheError::Query(QueryError::GetValue(e)))
            .map(|id| {
                id.try_into()
                    .expect("All values must be u64 ids in this database")
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_id_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap();
        let writer = IdDatabase::new(&mut env, true).unwrap();
        let reader = IdDatabase::new(&mut env, false).unwrap();

        let key = b"key";

        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();
        let id = writer.get_or_generate(txn.txn_mut(), Some(key)).unwrap();
        writer.get_or_generate(txn.txn_mut(), None).unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(writer.get(txn.txn(), key).unwrap(), id);
        assert_eq!(reader.get(txn.txn(), key).unwrap(), id);
        txn.commit_and_renew().unwrap();
    }
}
