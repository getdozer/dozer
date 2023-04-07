mod lmdb;
use std::fmt::Debug;

use self::expression::QueryExpression;
use crate::errors::CacheError;
use dozer_types::models::api_endpoint::ConflictResolution;
use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{IndexDefinition, Record, Schema, SchemaWithIndex},
};
pub use lmdb::cache_manager::{CacheManagerOptions, LmdbRoCacheManager, LmdbRwCacheManager};
pub mod expression;
pub mod index;
mod plan;
pub mod test_utils;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct CacheRecord {
    pub id: u64,
    pub version: u32,
    pub record: Record,
}

impl CacheRecord {
    pub fn new(id: u64, version: u32, record: Record) -> Self {
        Self {
            id,
            version,
            record,
        }
    }
}

pub trait RoCacheManager: Send + Sync + Debug {
    /// Opens a cache in read-only mode with given name or an alias with that name.
    ///
    /// If the name is both an alias and a real name, it's treated as an alias.
    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError>;
}

pub trait RwCacheManager: RoCacheManager {
    /// Opens a cache in read-write mode with given name or an alias with that name.
    ///
    /// If the name is both an alias and a real name, it's treated as an alias.
    fn open_rw_cache(
        &self,
        name: &str,
        conflict_resolution: ConflictResolution,
    ) -> Result<Option<Box<dyn RwCache>>, CacheError>;

    /// Creates a new cache with given `schema`s, which can also be opened in read-only mode using `open_ro_cache`.
    ///
    /// Schemas cannot be changed after the cache is created.
    ///
    /// The cache's name is unique.
    fn create_cache(
        &self,
        schema: Schema,
        indexes: Vec<IndexDefinition>,
        conflict_resolution: ConflictResolution,
    ) -> Result<Box<dyn RwCache>, CacheError>;

    /// Creates an alias `alias` for a cache with name `name`.
    ///
    /// If `alias` already exists, it's overwritten. If cache with name `name` doesn't exist, the alias is still recorded.
    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError>;
}

pub trait RoCache: Send + Sync + Debug {
    /// Returns the name of the cache.
    fn name(&self) -> &str;

    // Schema Operations
    fn get_schema(&self) -> &SchemaWithIndex;

    // Record Operations
    fn get(&self, key: &[u8]) -> Result<CacheRecord, CacheError>;
    fn count(&self, query: &QueryExpression) -> Result<usize, CacheError>;
    fn query(&self, query: &QueryExpression) -> Result<Vec<CacheRecord>, CacheError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct RecordMeta {
    pub id: u64,
    pub version: u32,
}

impl RecordMeta {
    pub fn new(id: u64, version: u32) -> Self {
        Self { id, version }
    }
}

#[derive(Debug)]
pub enum UpsertResult {
    Updated {
        old_meta: RecordMeta,
        new_meta: RecordMeta,
    },
    Inserted {
        meta: RecordMeta,
    },
    Ignored,
}

pub trait RwCache: RoCache {
    /// Inserts a record into the cache. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Depending on the `ConflictResolution` strategy, it may or may not overwrite the existing record.
    fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError>;

    /// Deletes a record. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Returns the id and version of the deleted record if it existed.
    fn delete(&mut self, key: &[u8]) -> Result<Option<RecordMeta>, CacheError>;

    /// Updates a record in the cache. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Depending on the `ConflictResolution` strategy, it may actually insert the record if it doesn't exist.
    fn update(&mut self, key: &[u8], record: &Record) -> Result<UpsertResult, CacheError>;

    /// Commits the current transaction.
    fn commit(&mut self) -> Result<(), CacheError>;
}
