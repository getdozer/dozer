use std::path::PathBuf;

use dozer_storage::errors::StorageError;
use dozer_types::thiserror::Error;
use dozer_types::{bincode, thiserror};

use dozer_types::errors::types::{DeserializationError, SerializationError, TypeError};
use dozer_types::types::{IndexDefinition, SchemaWithIndex};

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

    #[error("Log error: {0}")]
    LogError(#[from] LogError),

    #[error("Storage error: {0}")]
    Storage(#[from] dozer_storage::errors::StorageError),
    #[error("Schema is not found")]
    SchemaNotFound,
    #[error("Schema for {name} mismatch: given {given:?}, stored {stored:?}")]
    SchemaMismatch {
        name: String,
        given: Box<SchemaWithIndex>,
        stored: Box<SchemaWithIndex>,
    },
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
    #[error("Primary key already exists")]
    PrimaryKeyExists,
    #[error("Cannot find log file {0:?}")]
    LogFileNotFound(PathBuf),
    #[error("Cannot read log {0:?}")]
    LogReadError(#[source] std::io::Error),
}

impl CacheError {
    pub fn map_serialization_error(e: dozer_types::bincode::Error) -> CacheError {
        CacheError::Type(TypeError::SerializationError(SerializationError::Bincode(
            e,
        )))
    }
    pub fn map_deserialization_error(e: dozer_types::bincode::Error) -> CacheError {
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
}

#[derive(Error, Debug)]
pub enum LogError {
    #[error("Error reading log: {0}")]
    ReadError(#[source] std::io::Error),
    #[error("Error seeking file log: {0},pos: {1}, error: {2}")]
    SeekError(String, u64, #[source] std::io::Error),
    #[error("Error deserializing log: {0}")]
    DeserializationError(#[from] bincode::Error),
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
    #[error("Matching index not found")]
    MatchingIndexNotFound,
}
