use lmdb::{Database, Environment, RwTransaction, Transaction, WriteFlags};

use crate::{
    cache::lmdb::{
        query::helper,
        utils::{self, DatabaseCreateOptions},
    },
    errors::{CacheError, QueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct IdDatabase(Database);

impl IdDatabase {
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

    pub fn get_or_generate(
        &self,
        txn: &mut RwTransaction,
        key: Option<&[u8]>,
    ) -> Result<[u8; 8], CacheError> {
        if let Some(key) = key {
            match txn.get(self.0, &key) {
                Ok(id) => Ok(id.try_into().unwrap()),
                Err(lmdb::Error::NotFound) => self.generate_id(txn, Some(key)),
                Err(e) => Err(CacheError::QueryError(QueryError::InsertValue(e))),
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
            .map_err(|e| CacheError::InternalError(Box::new(e)))?
            .ms_entries as u64;
        let id = id.to_be_bytes();

        let key = key.unwrap_or(&id);

        txn.put(self.0, &key, &id, WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;

        Ok(id)
    }

    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<[u8; 8], CacheError> {
        txn.get(self.0, &key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))
            .map(|id| id.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_id_database() {
        let env = init_env(&CacheOptions::default()).unwrap();
        let writer = IdDatabase::new(&env, true).unwrap();
        let reader = IdDatabase::new(&env, false).unwrap();

        let key = b"key";

        let mut txn = env.begin_rw_txn().unwrap();
        let id = writer.get_or_generate(&mut txn, Some(key)).unwrap();
        writer.get_or_generate(&mut txn, None).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert_eq!(writer.get(&txn, key).unwrap(), id);
        assert_eq!(reader.get(&txn, key).unwrap(), id);
        txn.commit().unwrap();
    }
}
