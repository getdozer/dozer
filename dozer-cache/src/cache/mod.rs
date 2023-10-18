mod lmdb;
use std::collections::HashSet;
use std::fmt::Debug;

use self::expression::QueryExpression;
use crate::errors::CacheError;
use dozer_tracing::Labels;
use dozer_types::models::api_endpoint::{
    OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
};
use dozer_types::node::SourceStates;
use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{Record, SchemaWithIndex},
};
pub use lmdb::cache_manager::{
    begin_dump_txn, dump, CacheManagerOptions, LmdbRoCacheManager, LmdbRwCacheManager,
};
pub mod expression;
mod index;
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
    /// Opens a cache in read-only mode, and attach given labels.
    fn open_ro_cache(
        &self,
        name_or_alias: String,
        labels: Labels,
    ) -> Result<Option<Box<dyn RoCache>>, CacheError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheWriteOptions {
    pub insert_resolution: OnInsertResolutionTypes,
    pub delete_resolution: OnDeleteResolutionTypes,
    pub update_resolution: OnUpdateResolutionTypes,
    pub detect_hash_collision: bool,
}

pub trait RwCacheManager: RoCacheManager {
    /// Opens a cache in read-write mode, and attach given labels.
    fn open_rw_cache(
        &self,
        name_or_alias: String,
        labels: Labels,
        write_options: CacheWriteOptions,
    ) -> Result<Option<Box<dyn RwCache>>, CacheError>;

    /// Creates a new cache with given `schema`s, which can also be opened in read-only mode using `open_ro_cache`.
    ///
    /// Schemas cannot be changed after the cache is created.
    ///
    /// The name must be unique and non-empty.
    fn create_cache(
        &self,
        name: String,
        labels: Labels,
        schema: SchemaWithIndex,
        connections: &HashSet<String>,
        write_options: CacheWriteOptions,
    ) -> Result<Box<dyn RwCache>, CacheError>;

    /// Creates an alias `alias` for a cache with name `name`.
    ///
    /// If `alias` already exists, it's overwritten. If cache with name `name` doesn't exist, the alias is still recorded.
    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError>;
}

pub trait RoCache: Send + Sync + Debug {
    /// Returns the name of the cache.
    fn name(&self) -> &str;
    /// Returns the labels of the cache.
    fn labels(&self) -> &Labels;

    // Schema Operations
    fn get_schema(&self) -> &SchemaWithIndex;

    // Record Operations
    fn get(&self, key: &[u8]) -> Result<CacheRecord, CacheError>;
    fn count(&self, query: &QueryExpression) -> Result<usize, CacheError>;
    fn query(&self, query: &QueryExpression) -> Result<Vec<CacheRecord>, CacheError>;

    // Cache metadata
    fn get_commit_state(&self) -> Result<Option<CommitState>, CacheError>;
    fn is_snapshotting_done(&self) -> Result<bool, CacheError>;
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct CommitState {
    pub source_states: SourceStates,
    pub log_position: u64,
}

pub trait RwCache: RoCache {
    /// Inserts a record into the cache. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Depending on the `ConflictResolution` strategy, it may or may not overwrite the existing record.
    fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError>;

    /// Deletes a record. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Returns the id and version of the deleted record if it existed.
    ///
    /// If the schema has primary index, only fields that are part of the primary index are used to identify the record.
    fn delete(&mut self, record: &Record) -> Result<Option<RecordMeta>, CacheError>;

    /// Updates a record in the cache. Implicitly starts a transaction if there's no active transaction.
    ///
    /// Depending on the `ConflictResolution` strategy, it may actually insert the record if it doesn't exist.
    ///
    /// If the schema has primary index, only fields that are part of the primary index are used to identify the old record.
    fn update(&mut self, old: &Record, record: &Record) -> Result<UpsertResult, CacheError>;

    /// Marks a connection as snapshotting done. Implicitly starts a transaction if there's no active transaction.
    fn set_connection_snapshotting_done(&mut self, connection_name: &str)
        -> Result<(), CacheError>;

    /// Commits the current transaction.
    fn commit(&mut self, state: &CommitState) -> Result<(), CacheError>;
}
