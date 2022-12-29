use lmdb::{Database, Environment, RwTransaction, Transaction, WriteFlags};

use crate::{
    cache::lmdb::utils::{self, DatabaseCreateOptions},
    errors::{CacheError, QueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct PrimaryIndexDatabase(Database);

impl PrimaryIndexDatabase {
    pub fn new(env: &Environment, create_if_not_exist: bool) -> Result<Self, CacheError> {
        let options = if create_if_not_exist {
            Some(DatabaseCreateOptions {
                allow_dup: false,
                fixed_length_key: false,
            })
        } else {
            None
        };
        let db = utils::init_db(env, Some("primary_index"), options)?;
        Ok(Self(db))
    }

    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: &[u8],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        if txn.get(self.0, &key).is_ok() {
            txn.del(self.0, &key, None)
                .map_err(|e| CacheError::QueryError(QueryError::DeleteValue(e)))?;
        }
        txn.put(self.0, &key, &id, WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;
        Ok(())
    }

    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<[u8; 8], CacheError> {
        txn.get(self.0, &key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))
            .map(|id| id.try_into().unwrap())
    }

    pub fn delete(&self, txn: &mut RwTransaction, key: &[u8]) -> Result<(), CacheError> {
        txn.del(self.0, &key, None)
            .map_err(|e| CacheError::QueryError(QueryError::DeleteValue(e)))
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_primary_index_database() {
        let env = init_env(&CacheOptions::default()).unwrap();
        let writer = PrimaryIndexDatabase::new(&env, true).unwrap();
        let reader = PrimaryIndexDatabase::new(&env, false).unwrap();

        let key = b"key";
        let id = [1u8; 8];

        let mut txn = env.begin_rw_txn().unwrap();
        writer.insert(&mut txn, key, id).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert_eq!(writer.get(&txn, key).unwrap(), id);
        assert_eq!(reader.get(&txn, key).unwrap(), id);
        txn.commit().unwrap();

        let mut txn = env.begin_rw_txn().unwrap();
        writer.delete(&mut txn, key).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert!(writer.get(&txn, key).is_err());
        assert!(reader.get(&txn, key).is_err());
        txn.commit().unwrap();
    }
}
