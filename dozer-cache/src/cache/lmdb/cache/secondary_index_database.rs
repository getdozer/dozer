use std::cmp::Ordering;

use dozer_types::types::{IndexDefinition, Schema};
use lmdb::{Database, Environment, RoCursor, RwTransaction, Transaction, WriteFlags};

use crate::{
    cache::lmdb::{
        comparator,
        query::helper::lmdb_cmp,
        utils::{self, DatabaseCreateOptions},
    },
    errors::{CacheError, LmdbQueryError},
};

#[derive(Debug, Clone, Copy)]
pub struct SecondaryIndexDatabase(Database);

impl SecondaryIndexDatabase {
    pub fn new(
        env: &Environment,
        schema: &Schema,
        index: usize,
        index_definition: &IndexDefinition,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let schema_id = schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        let name = format!("index_#{}_#{}_#{}", schema_id.id, schema_id.version, index);
        let options = if create_if_not_exist {
            Some(DatabaseCreateOptions {
                allow_dup: true,
                fixed_length_key: false,
            })
        } else {
            None
        };
        let db = utils::init_db(env, Some(&name), options)?;

        if let IndexDefinition::SortedInverted(fields) = index_definition {
            comparator::set_sorted_inverted_comparator(env, db, fields)
                .map_err(|e| CacheError::InternalError(Box::new(e)))?;
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
            .map_err(|e| CacheError::QueryError(LmdbQueryError::InsertValue(e)))
    }

    #[cfg(test)]
    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<[u8; 8], CacheError> {
        txn.get(self.0, &key)
            .map_err(|e| CacheError::QueryError(LmdbQueryError::GetValue(e)))
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
            .map_err(|e| CacheError::QueryError(LmdbQueryError::DeleteValue(e)))
    }

    pub fn open_ro_cursor<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<RoCursor<'txn>, CacheError> {
        txn.open_ro_cursor(self.0)
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }

    pub fn cmp<T: Transaction>(&self, txn: &T, a: &[u8], b: &[u8]) -> Ordering {
        lmdb_cmp(txn, self.0, a, b)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{lmdb::utils::init_env, test_utils::schema_1, CacheOptions};

    use super::*;

    #[test]
    fn test_secondary_index_database() {
        let env = init_env(&CacheOptions::default()).unwrap();
        let (schema, secondary_indexes) = schema_1();
        let writer =
            SecondaryIndexDatabase::new(&env, &schema, 0, &secondary_indexes[0], true).unwrap();
        let reader =
            SecondaryIndexDatabase::new(&env, &schema, 0, &secondary_indexes[0], false).unwrap();

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
        writer.delete(&mut txn, key, id).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_txn().unwrap();
        assert!(writer.get(&txn, key).is_err());
        assert!(reader.get(&txn, key).is_err());
        txn.commit().unwrap();
    }
}
