use std::collections::HashSet;
use std::path::PathBuf;

use dozer_storage::errors::StorageError;
use dozer_storage::RestoreError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

use dozer_log::errors::ReaderError;
use dozer_types::errors::types::{DeserializationError, SerializationError, TypeError};
use dozer_types::types::{Field, IndexDefinition, SchemaWithIndex};

use crate::cache::RecordMeta;

#[derive(Debug)]
pub struct ConnectionMismatch {
    pub name: String,
    pub given: HashSet<String>,
    pub stored: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Io error on {0:?}: {1}")]
    Io(PathBuf, #[source] std::io::Error),
    #[error("Query error: {0}")]
    Query(#[from] QueryError),
    #[error("Index error: {0}")]
    Index(#[from] IndexError),
    #[error("Plan error: {0}")]
    Plan(#[from] PlanError),
    #[error("Type error: {0}")]
    Type(#[from] TypeError),
    #[error("Restore error: {0}")]
    Restore(#[from] RestoreError),

    #[error("Log error: {0}")]
    ReaderError(#[from] ReaderError),

    #[error("Storage error: {0}")]
    Storage(#[from] dozer_storage::errors::StorageError),
    #[error("Cache name is empty")]
    EmptyName,
    #[error("Schema is not found")]
    SchemaNotFound,
    #[error("Schema for {name} mismatch: given {given:?}, stored {stored:?}")]
    SchemaMismatch {
        name: String,
        given: Box<SchemaWithIndex>,
        stored: Box<SchemaWithIndex>,
    },
    #[error("Connections for {} mismatch, give: {:?}, stored: {:?})", .0.name, .0.given, .0.stored)]
    ConnectionsMismatch(Box<ConnectionMismatch>),
    #[error("Index definition {0} is not found")]
    IndexDefinitionNotFound(String),
    #[error("Index definition {name} mismatch: given {given:?}, stored {stored:?}")]
    IndexDefinitionMismatch {
        name: String,
        given: IndexDefinition,
        stored: IndexDefinition,
    },
    #[error("Path not initialized for Cache Reader")]
    PathNotInitialized,
    #[error("Attempt to delete or update a cache with append-only schema")]
    AppendOnlySchema,
    #[error("Primary key is not found")]
    PrimaryKeyNotFound,
    #[error("Primary key {key:?} already exists: record id {}, version {}, insert operation id {insert_operation_id}", .meta.id, .meta.version)]
    PrimaryKeyExists {
        key: Vec<(String, Field)>,
        meta: RecordMeta,
        insert_operation_id: u64,
    },
    #[error("Internal thread panic: {0}")]
    InternalThreadPanic(#[source] tokio::task::JoinError),
}

impl CacheError {
    pub fn map_serialization_error(e: dozer_types::bincode::error::EncodeError) -> CacheError {
        CacheError::Type(TypeError::SerializationError(SerializationError::Bincode(
            e,
        )))
    }
    pub fn map_deserialization_error(e: dozer_types::bincode::error::DecodeError) -> CacheError {
        CacheError::Type(TypeError::DeserializationError(
            DeserializationError::Bincode(e),
        ))
    }

    pub fn is_map_full(&self) -> bool {
        matches!(
            self,
            CacheError::Storage(StorageError::Lmdb(dozer_storage::lmdb::Error::MapFull))
        )
    }

    pub fn is_key_size(&self) -> bool {
        matches!(
            self,
            CacheError::Storage(StorageError::Lmdb(dozer_storage::lmdb::Error::BadValSize))
        )
    }
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Failed to get a record by id - {0:?}")]
    GetValue(#[source] dozer_storage::lmdb::Error),
    #[error("Failed to get a schema by id - {0:?}")]
    GetSchema(#[source] dozer_storage::lmdb::Error),
    #[error("Failed to insert a record - {0:?}")]
    InsertValue(#[source] dozer_storage::lmdb::Error),
    #[error("Failed to delete a record - {0:?}")]
    DeleteValue(#[source] dozer_storage::lmdb::Error),
}

#[derive(Error, Debug)]
pub enum CompareError {
    #[error("cannot read field length")]
    CannotReadFieldLength,
    #[error("cannot read field")]
    CannotReadField,
    #[error("invalid sort direction")]
    InvalidSortDirection(u8),
    #[error("deserialization error: {0:?}")]
    DeserializationError(#[from] DeserializationError),
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("field indexes dont match with index_scan")]
    MismatchedIndexAndValues,
    #[error("Expected strings for full text search")]
    ExpectedStringFullText,
    #[error("Field index out of range")]
    FieldIndexOutOfRange,
    #[error("Full text index generates one key for each field")]
    IndexSingleField,
    #[error("Field {0} cannot be indexed using full text")]
    FieldNotCompatibleIndex(usize),
    #[error("No secondary indexes defined")]
    MissingSecondaryIndexes,
    #[error("Unsupported Index: {0}")]
    UnsupportedIndex(String),
    #[error("range queries on multiple fields are not supported ")]
    UnsupportedMultiRangeIndex,
    #[error("Compound_index is required for fields: {0}")]
    MissingCompoundIndex(String),
}

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("Field {0:?} not found in query")]
    FieldNotFound(String),
    #[error("Type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("Cannot sort full text filter")]
    CannotSortFullTextFilter,
    #[error("Conflicting sort options")]
    ConflictingSortOptions,
    #[error("Cannot have more than one range query")]
    RangeQueryLimit,
    #[error("Matching index not found. Try to add following secondary index configuration:\n{0}")]
    MatchingIndexNotFound(String),
}
