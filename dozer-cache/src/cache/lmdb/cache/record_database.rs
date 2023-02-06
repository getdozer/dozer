use dozer_storage::{
    lmdb::{Database, DatabaseFlags, RoCursor, RwTransaction, Transaction, WriteFlags},
    lmdb_storage::LmdbEnvironmentManager,
};
use dozer_types::{bincode, types::Record};

use crate::{
    cache::lmdb::query::helper,
    errors::{CacheError, QueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct RecordDatabase(Database);

impl RecordDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let flags = if create_if_not_exist {
            Some(DatabaseFlags::INTEGER_KEY)
        } else {
            None
        };
        let db = env.create_database(Some("records"), flags)?;
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
            .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))
    }

    pub fn get<T: Transaction>(&self, txn: &T, id: [u8; 8]) -> Result<Record, CacheError> {
        helper::get(txn, self.0, &id)
    }

    pub fn delete(&self, txn: &mut RwTransaction, id: [u8; 8]) -> Result<(), CacheError> {
        txn.del(self.0, &id, None)
            .map_err(|e| CacheError::Query(QueryError::DeleteValue(e)))
    }

    pub fn count(&self, txn: &impl Transaction) -> Result<usize, CacheError> {
        helper::lmdb_stat(txn, self.0)
            .map(|stat| stat.ms_entries)
            .map_err(|e| CacheError::Internal(Box::new(e)))
    }

    pub fn open_ro_cursor<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<RoCursor<'txn>, CacheError> {
        txn.open_ro_cursor(self.0)
            .map_err(|e| CacheError::Internal(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb_storage::LmdbTransaction;

    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_record_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap();
        let writer = RecordDatabase::new(&mut env, true).unwrap();
        let reader = RecordDatabase::new(&mut env, false).unwrap();
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();
        assert_eq!(writer.count(txn.txn()).unwrap(), 0);
        assert_eq!(reader.count(txn.txn()).unwrap(), 0);
        txn.commit_and_renew().unwrap();

        let id = 1u64;
        let record = Record::new(None, vec![], None);

        writer
            .insert(txn.txn_mut(), id.to_be_bytes(), &record)
            .unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(writer.count(txn.txn()).unwrap(), 1);
        assert_eq!(reader.count(txn.txn()).unwrap(), 1);
        assert_eq!(writer.get(txn.txn(), id.to_be_bytes()).unwrap(), record);
        assert_eq!(reader.get(txn.txn(), id.to_be_bytes()).unwrap(), record);
        txn.commit_and_renew().unwrap();

        writer.delete(txn.txn_mut(), id.to_be_bytes()).unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(writer.count(txn.txn()).unwrap(), 0);
        assert_eq!(reader.count(txn.txn()).unwrap(), 0);
        assert!(writer.get(txn.txn(), id.to_be_bytes()).is_err());
        assert!(reader.get(txn.txn(), id.to_be_bytes()).is_err());
        txn.commit_and_renew().unwrap();
    }
}
