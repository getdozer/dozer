use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use dozer_types::bincode;

use lmdb::{Database, Environment, RoTransaction, RwTransaction, Transaction, WriteFlags};

use dozer_types::types::{IndexDefinition, Record};
use dozer_types::types::{Schema, SchemaIdentifier};

use super::super::Cache;
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::query::helper;
use super::{comparator, utils, CacheOptions};
use crate::cache::expression::QueryExpression;
use crate::cache::index;
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
    primary_index: Database,
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
    pub fn init_txn(&self) -> RwTransaction {
        let txn = self.env.begin_rw_txn().unwrap();
        txn
    }
    pub fn new(cache_options: CacheOptions) -> Result<Self, CacheError> {
        let env = utils::init_env(&cache_options)?;
        let db = utils::init_db(&env, Some("records"), &cache_options, false, true)?;
        let primary_index =
            utils::init_db(&env, Some("primary_index"), &cache_options, false, false)?;
        let schema_db = utils::init_db(&env, Some("schemas"), &cache_options, false, false)?;
        Ok(Self {
            env,
            db,
            primary_index,
            index_metadata: Arc::new(IndexMetaData::default()),
            schema_db,
            cache_options,
        })
    }

    pub fn _insert(
        &self,
        txn: &mut RwTransaction,
        rec: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        let id = helper::lmdb_stat(txn, self.db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?
            .ms_entries as u64;
        let encoded: Vec<u8> =
            bincode::serialize(&rec).map_err(CacheError::map_serialization_error)?;

        txn.put(
            self.db,
            &id.to_be_bytes().as_slice(),
            &encoded.as_slice(),
            WriteFlags::default(),
        )
        .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;

        let indexer = Indexer {
            primary_index: self.primary_index,
            index_metadata: self.index_metadata.clone(),
        };

        indexer.build_indexes(txn, rec, schema, secondary_indexes, id)?;

        Ok(())
    }

    pub fn _insert_schema(
        &self,
        txn: &mut RwTransaction,
        schema: &Schema,
        name: &str,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        let encoded: Vec<u8> = bincode::serialize(&(schema, secondary_indexes))
            .map_err(CacheError::map_serialization_error)?;
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
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let schema_reverse_key = index::get_schema_reverse_key(name);
        let schema_identifier = txn
            .get(self.schema_db, &schema_reverse_key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)
            .map_err(CacheError::map_deserialization_error)?;

        let schema = self.get_schema_and_indexes(txn, &schema_id)?;

        Ok(schema)
    }

    fn get_schema_and_indexes<T: Transaction>(
        &self,
        txn: &T,
        schema_identifier: &SchemaIdentifier,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let key = get_schema_key(schema_identifier);
        let schema = txn
            .get(self.schema_db, &key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
        let schema = bincode::deserialize(schema).map_err(CacheError::map_deserialization_error)?;
        Ok(schema)
    }

    fn _delete_internal(&self, key: &[u8], txn: &mut RwTransaction) -> Result<(), lmdb::Error> {
        let id: [u8; 8] = txn.get(self.primary_index, &key)?.try_into().unwrap();
        txn.del(self.db, &id, None)?;
        txn.del(self.primary_index, &key, None)?;
        Ok(())
    }

    pub fn _delete(&self, key: &[u8], txn: &mut RwTransaction) -> Result<(), CacheError> {
        self._delete_internal(key, txn)
            .map_err(|e| CacheError::QueryError(QueryError::DeleteValue(e)))
    }

    pub fn _update(
        &self,
        key: &[u8],
        rec: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
        txn: &mut RwTransaction,
    ) -> Result<(), CacheError> {
        self._delete(key, txn)?;

        self._insert(txn, rec, schema, secondary_indexes)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
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
        let (schema, secondary_indexes) = self.get_schema_and_indexes(&txn, &schema_identifier)?;

        self._insert(&mut txn, rec, &schema, &secondary_indexes)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        self._delete(key, &mut txn)?;

        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let id = txn
            .get(self.primary_index, &key)
            .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
        helper::get(&txn, self.db, id)
    }

    fn query(&self, name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let (schema, secondary_indexes) = self._get_schema_from_reverse_key(name, &txn)?;

        let handler = LmdbQueryHandler::new(
            &self.db,
            self.index_metadata.clone(),
            &txn,
            &schema,
            &secondary_indexes,
            query,
        );
        let records = handler.query()?;
        Ok(records)
    }

    fn update(&self, key: &[u8], rec: &Record, _schema: &Schema) -> Result<(), CacheError> {
        let schema_identifier = rec
            .schema_id
            .to_owned()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let (schema, secondary_indexes) = self.get_schema_and_indexes(&txn, &schema_identifier)?;
        self._update(key, rec, &schema, &secondary_indexes, &mut txn)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
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
        self.get_schema_and_indexes(&txn, schema_identifier)
            .map(|(schema, _)| schema)
    }
    fn insert_schema(
        &self,
        name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        // Create a db for each index
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let key = IndexMetaData::get_key(schema, idx);
            let name = format!("index_#{}", key);
            let db = utils::init_db(&self.env, Some(&name), &self.cache_options, true, false)?;

            if let IndexDefinition::SortedInverted(fields) = index {
                comparator::set_sorted_inverted_comparator(&self.env, db, schema, fields)
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?;
            }

            self.index_metadata.insert_index(key, db);
        }

        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self._insert_schema(&mut txn, schema, name, secondary_indexes)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }
}
