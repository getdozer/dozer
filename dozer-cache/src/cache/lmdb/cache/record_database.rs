use dozer_types::{bincode, types::Record};
use lmdb::{Database, Environment, RoCursor, RwTransaction, Transaction, WriteFlags};

use crate::{
    cache::lmdb::{
        query::helper,
        utils::{self, DatabaseCreateOptions},
    },
    errors::{CacheError, QueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct RecordDatabase(Database);

impl RecordDatabase {
    pub fn new(env: &Environment, create_if_not_exist: bool) -> Result<Self, CacheError> {
        let options = if create_if_not_exist {
            Some(DatabaseCreateOptions {
                allow_dup: false,
                fixed_length_key: true,
            })
        } else {
            None
        };
        let db = utils::init_db(env, Some("records"), options)?;
        Ok(Self(db))
    }

    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        id: [u8; 8],
        record: &Record,
    ) -> Result<(), CacheError> {
        let encoded: Vec<u8> =
            bincode::serialize(&record).map_err(CacheError::map_serialization_error)?;

        txn.put(self.0, &id, &encoded.as_slice(), WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))
    }

    pub fn get<T: Transaction>(&self, txn: &T, id: [u8; 8]) -> Result<Record, CacheError> {
        helper::get(txn, self.0, &id)
    }

    pub fn delete(&self, txn: &mut RwTransaction, id: [u8; 8]) -> Result<(), CacheError> {
        txn.del(self.0, &id, None)
            .map_err(|e| CacheError::QueryError(QueryError::DeleteValue(e)))
    }

    pub fn count(&self, txn: &impl Transaction) -> Result<usize, CacheError> {
        helper::lmdb_stat(txn, self.0)
            .map(|stat| stat.ms_entries)
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }

    pub fn open_ro_cursor<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<RoCursor<'txn>, CacheError> {
        txn.open_ro_cursor(self.0)
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_record_database() {
        let env = init_env(&CacheOptions::default()).unwrap();
        let writer = RecordDatabase::new(&env, true).unwrap();
        let reader = RecordDatabase::new(&env, false).unwrap();
        let txn = env.begin_ro_txn().unwrap();
        assert_eq!(writer.count(&txn).unwrap(), 0);
        assert_eq!(reader.count(&txn).unwrap(), 0);
        txn.commit().unwrap();

        let id = 1u64;
        let record = Record::new(None, vec![], None);

        let mut txn = env.begin_rw_txn().unwrap();
        writer.insert(&mut txn, id.to_be_bytes(), &record).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert_eq!(writer.count(&txn).unwrap(), 1);
        assert_eq!(reader.count(&txn).unwrap(), 1);
        assert_eq!(writer.get(&txn, id.to_be_bytes()).unwrap(), record);
        assert_eq!(reader.get(&txn, id.to_be_bytes()).unwrap(), record);
        txn.commit().unwrap();

        let mut txn = env.begin_rw_txn().unwrap();
        writer.delete(&mut txn, id.to_be_bytes()).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert_eq!(writer.count(&txn).unwrap(), 0);
        assert_eq!(reader.count(&txn).unwrap(), 0);
        assert!(writer.get(&txn, id.to_be_bytes()).is_err());
        assert!(reader.get(&txn, id.to_be_bytes()).is_err());
        txn.commit().unwrap();
    }
}
