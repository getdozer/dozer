use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use dozer_types::bincode;

use lmdb::{
    Database, Environment, Error as LmdbError, RoTransaction, RwTransaction, Transaction,
    WriteFlags,
};

use dozer_types::types::{IndexDefinition, Record};
use dozer_types::types::{Schema, SchemaIdentifier};
use lmdb_sys::{mdb_set_compare, MDB_val, MDB_SUCCESS};

use super::super::Cache;
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::query::helper;
use super::{utils, CacheOptions};
use crate::cache::expression::QueryExpression;
use crate::cache::index::{self, compare_secondary_index};
use crate::errors::{CacheError, QueryError};

pub struct IndexMetaData {
    //schema_id, secondary_key
    indexes: RwLock<HashMap<usize, Database>>,
}
impl Default for IndexMetaData {
    fn default() -> Self {
        Self::new()
    }
}
impl IndexMetaData {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_key(schema: &Schema, idx: usize) -> usize {
        schema.identifier.as_ref().unwrap().id as usize * 100000 + idx
    }

    pub fn insert_index(&self, key: usize, db: Database) {
        self.indexes.write().map(|mut h| h.insert(key, db)).unwrap();
    }
    pub fn get_db(&self, schema: &Schema, idx: usize) -> Database {
        let key = Self::get_key(schema, idx);
        self.indexes.read().unwrap().get(&key).unwrap().to_owned()
    }
    pub fn get_all_raw(&self) -> HashMap<usize, Database> {
        self.indexes.read().unwrap().to_owned()
    }
}

pub struct LmdbCache {
    env: Environment,
    db: Database,
    index_metadata: Arc<IndexMetaData>,
    schema_db: Database,
    cache_options: CacheOptions,
}

pub fn get_schema_key(schema_id: &SchemaIdentifier) -> Vec<u8> {
    [
        "sc".as_bytes(),
        schema_id.id.to_be_bytes().as_ref(),
        schema_id.version.to_be_bytes().as_ref(),
    ]
    .join("#".as_bytes())
}

impl LmdbCache {
    pub fn new(cache_options: CacheOptions) -> Result<Self, CacheError> {
        let env = utils::init_env(&cache_options)?;
        let db = utils::init_db(&env, Some("records"), &cache_options)?;
        let schema_db = utils::init_db(&env, Some("schemas"), &cache_options)?;
        Ok(Self {
            env,
            db,
            index_metadata: Arc::new(IndexMetaData::default()),
            schema_db,
            cache_options,
        })
    }

    fn _insert(
        &self,
        txn: &mut RwTransaction,
        rec: &Record,
        schema: &Schema,
    ) -> Result<(), CacheError> {
        let p_key = &schema.primary_index;
        let values = &rec.values;
        let key = index::get_primary_key(p_key, values);
        let encoded: Vec<u8> =
            bincode::serialize(&rec).map_err(CacheError::map_serialization_error)?;

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())
            .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;

        let indexer = Indexer {
            index_metadata: self.index_metadata.clone(),
        };

        indexer.build_indexes(txn, rec, schema, key)?;

        Ok(())
    }

    fn _insert_schema(
        &self,
        txn: &mut RwTransaction,
        schema: &Schema,
        name: &str,
    ) -> Result<(), CacheError> {
        let encoded: Vec<u8> =
            bincode::serialize(&schema).map_err(CacheError::map_serialization_error)?;
        let schema_id = schema.to_owned().identifier.unwrap();
        let key = get_schema_key(&schema_id);

        // Insert Schema with {id, version}
        txn.put::<Vec<u8>, Vec<u8>>(self.schema_db, &key, &encoded, WriteFlags::default())
            .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;

        let schema_id_bytes =
            bincode::serialize(&schema_id).map_err(CacheError::map_serialization_error)?;

        // Insert Reverse key lookup for schema by name
        let schema_key = index::get_schema_reverse_key(name);

        txn.put::<Vec<u8>, Vec<u8>>(
            self.schema_db,
            &schema_key,
            &schema_id_bytes,
            WriteFlags::default(),
        )
        .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;

        Ok(())
    }

    pub fn get_index_metadata(&self) -> (&Environment, Arc<IndexMetaData>) {
        (&self.env, self.index_metadata.clone())
    }
    pub fn get_db(&self) -> (&Environment, &Database) {
        (&self.env, &self.db)
    }
    pub fn get_schema_db(&self) -> (&Environment, &Database) {
        (&self.env, &self.schema_db)
    }

    fn _get_schema_from_reverse_key(
        &self,
        name: &str,
        txn: &RoTransaction,
    ) -> Result<Schema, CacheError> {
        let schema_reverse_key = index::get_schema_reverse_key(name);
        let schema_identifier = txn
            .get(self.schema_db, &schema_reverse_key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)
            .map_err(CacheError::map_deserialization_error)?;

        let schema = self._get_schema(txn, &schema_id)?;

        Ok(schema)
    }

    fn _get_schema(
        &self,
        txn: &RoTransaction,
        schema_identifier: &SchemaIdentifier,
    ) -> Result<Schema, CacheError> {
        let key = get_schema_key(schema_identifier);
        let schema = txn
            .get(self.schema_db, &key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
        let schema: Schema =
            bincode::deserialize(schema).map_err(CacheError::map_deserialization_error)?;
        Ok(schema)
    }

    fn _delete(&self, key: &[u8]) -> Result<(), LmdbError> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }
}

impl Cache for LmdbCache {
    fn insert(&self, rec: &Record) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema_identifier = rec
            .schema_id
            .to_owned()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;
        let schema = self.get_schema(&schema_identifier)?;

        self._insert(&mut txn, rec, &schema)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        self._delete(key)
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }

    fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let rec: Record = helper::get(&txn, self.db, key)?;
        Ok(rec)
    }

    fn query(&self, name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;

        let handler =
            LmdbQueryHandler::new(&self.db, self.index_metadata.clone(), &txn, &schema, query);
        let records = handler.query()?;
        Ok(records)
    }

    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        txn.del(self.db, &key, None)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get_schema_by_name(&self, name: &str) -> Result<Schema, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;
        Ok(schema)
    }

    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self._get_schema(&txn, schema_identifier)
    }
    fn insert_schema(&self, name: &str, schema: &Schema) -> Result<(), CacheError> {
        // Create a db for each index
        for (idx, index) in schema.secondary_indexes.iter().enumerate() {
            let key = IndexMetaData::get_key(schema, idx);
            let name = format!("index_#{}", key);
            let db = utils::init_db(&self.env, Some(&name), &self.cache_options)?;

            if let IndexDefinition::SortedInverted(_) = index {
                let txn = self
                    .env
                    .begin_rw_txn()
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?;
                unsafe {
                    assert_eq!(
                        mdb_set_compare(txn.txn(), db.dbi(), Some(compare_sorted_inverted_key)),
                        MDB_SUCCESS
                    );
                }
                txn.commit()
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?;
            }

            self.index_metadata.insert_index(key, db);
        }

        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self._insert_schema(&mut txn, schema, name)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }
}

unsafe fn mdb_val_to_slice(val: &MDB_val) -> &[u8] {
    std::slice::from_raw_parts(val.mv_data as *const u8, val.mv_size)
}

unsafe extern "C" fn compare_sorted_inverted_key(
    a: *const MDB_val,
    b: *const MDB_val,
) -> std::ffi::c_int {
    match compare_secondary_index(mdb_val_to_slice(&*a), mdb_val_to_slice(&*b)) {
        Ok(ordering) => ordering as std::ffi::c_int,
        Err(e) => {
            dozer_types::log::error!("Error deserializing secondary index key: {}", e);
            0
        }
    }
}
