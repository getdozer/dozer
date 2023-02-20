use std::cmp::Ordering;

use dozer_storage::{
    errors::StorageError,
    lmdb::{Database, DatabaseFlags, RoCursor, RwTransaction, Transaction, WriteFlags},
    lmdb_storage::{LmdbEnvironmentManager, LmdbExclusiveTransaction},
};
use dozer_types::types::{IndexDefinition, SchemaIdentifier};

use super::helper::lmdb_cmp;
use crate::{
    cache::lmdb::comparator,
    errors::{CacheError, QueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct SecondaryIndexDatabase(Database);

impl SecondaryIndexDatabase {
    pub fn open(
        env: &mut LmdbEnvironmentManager,
        schema_id: &SchemaIdentifier,
        index: usize,
        index_definition: &IndexDefinition,
    ) -> Result<Self, CacheError> {
        let name = format!("index_#{}_#{}_#{}", schema_id.id, schema_id.version, index);
        let db = env.create_database(Some(&name), None)?;

        let txn = env.begin_ro_txn()?;

        if let IndexDefinition::SortedInverted(fields) = index_definition {
            comparator::set_sorted_inverted_comparator(&txn, db, fields)?;
        }

        txn.commit().map_err(StorageError::InternalDbError)?;

        Ok(Self(db))
    }

    pub fn create(
        txn: &mut LmdbExclusiveTransaction,
        schema_id: &SchemaIdentifier,
        index: usize,
        index_definition: &IndexDefinition,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let name = format!("index_#{}_#{}_#{}", schema_id.id, schema_id.version, index);
        let flags = if create_if_not_exist {
            Some(DatabaseFlags::DUP_SORT)
        } else {
            None
        };

        let db = txn.create_database(Some(&name), flags)?;

        if let IndexDefinition::SortedInverted(fields) = index_definition {
            comparator::set_sorted_inverted_comparator(txn.txn(), db, fields)?;
        }

        Ok(Self(db))
    }

    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: &[u8],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        txn.put(self.0, &key, &id, WriteFlags::default())
            .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))
    }

    #[cfg(test)]
    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<[u8; 8], CacheError> {
        txn.get(self.0, &key)
            .map_err(|e| CacheError::Query(QueryError::GetValue(e)))
            .map(|id| {
                id.try_into()
                    .expect("All values must be u64 ids in this database")
            })
    }

    pub fn delete(
        &self,
        txn: &mut RwTransaction,
        key: &[u8],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        txn.del(self.0, &key, Some(&id))
            .map_err(|e| CacheError::Query(QueryError::DeleteValue(e)))
    }

    pub fn open_ro_cursor<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<RoCursor<'txn>, CacheError> {
        txn.open_ro_cursor(self.0)
            .map_err(|e| CacheError::Internal(Box::new(e)))
    }

    pub fn cmp<T: Transaction>(&self, txn: &T, a: &[u8], b: &[u8]) -> Ordering {
        lmdb_cmp(txn, self.0, a, b)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{
        lmdb::utils::{init_env, CacheOptions},
        test_utils::schema_1,
    };

    use super::*;

    #[test]
    fn test_secondary_index_database() {
        let env = init_env(&CacheOptions::default()).unwrap().0;
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();
        let (schema, secondary_indexes) = schema_1();
        let schema_id = schema.identifier.as_ref().unwrap();
        let writer =
            SecondaryIndexDatabase::create(&mut txn, schema_id, 0, &secondary_indexes[0], true)
                .unwrap();
        let reader =
            SecondaryIndexDatabase::create(&mut txn, schema_id, 0, &secondary_indexes[0], false)
                .unwrap();

        let key = b"key";
        let id = [1u8; 8];

        writer.insert(txn.txn_mut(), key, id).unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(writer.get(txn.txn(), key).unwrap(), id);
        assert_eq!(reader.get(txn.txn(), key).unwrap(), id);
        txn.commit_and_renew().unwrap();

        writer.delete(txn.txn_mut(), key, id).unwrap();
        txn.commit_and_renew().unwrap();

        assert!(writer.get(txn.txn(), key).is_err());
        assert!(reader.get(txn.txn(), key).is_err());
        txn.commit_and_renew().unwrap();
    }
}
