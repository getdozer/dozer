mod lmdb;
use std::fmt::Debug;

use self::expression::QueryExpression;
use crate::errors::CacheError;
use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{IndexDefinition, Record, Schema, SchemaIdentifier},
};
pub use lmdb::cache_manager::{CacheManagerOptions, LmdbCacheManager};
pub mod expression;
pub mod index;
mod plan;
pub mod test_utils;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct RecordWithId {
    pub id: u64,
    pub record: Record,
}

impl RecordWithId {
    pub fn new(id: u64, record: Record) -> Self {
        Self { id, record }
    }
}

pub trait CacheManager: Send + Sync + Debug {
    /// Opens a cache in read-write mode with given name or an alias with that name.
    ///
    /// If the name is both an alias and a real name, it's treated as an alias.
    fn open_rw_cache(&self, name: &str) -> Result<Option<Box<dyn RwCache>>, CacheError>;

    /// Opens a cache in read-only mode with given name or an alias with that name.
    ///
    /// If the name is both an alias and a real name, it's treated as an alias.
    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError>;

    /// Creates a new cache, which can also be opened in read-only mode using `open_ro_cache`.
    ///
    /// The cache's name is unique.
    fn create_cache(&self) -> Result<Box<dyn RwCache>, CacheError>;

    /// Creates an alias `alias` for a cache with name `name`.
    ///
    /// If `alias` already exists, it's overwritten. If cache with name `name` doesn't exist, the alias is still recorded.
    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError>;
}

pub trait RoCache: Send + Sync + Debug {
    /// Returns the name of the cache.
    fn name(&self) -> &str;

    // Schema Operations
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError>;
    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError>;

    // Record Operations
    fn get(&self, key: &[u8]) -> Result<RecordWithId, CacheError>;
    fn count(&self, schema_name: &str, query: &QueryExpression) -> Result<usize, CacheError>;
    fn query(
        &self,
        schema_name: &str,
        query: &QueryExpression,
    ) -> Result<Vec<RecordWithId>, CacheError>;
}

pub trait RwCache: RoCache {
    // Schema Operations
    fn insert_schema(
        &self,
        name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;

    // Record Operations
    /// Sets the version of the inserted record and inserts it into the cache.
    fn insert(&self, record: &mut Record) -> Result<(), CacheError>;
    /// Returns version of the deleted record.
    fn delete(&self, key: &[u8]) -> Result<u32, CacheError>;
    /// Sets the version of the updated record and updates it in the cache. Returns the version of the record before the update.
    fn update(&self, key: &[u8], record: &mut Record) -> Result<u32, CacheError>;
    /// Commits the current transaction.
    fn commit(&self) -> Result<(), CacheError>;
}
