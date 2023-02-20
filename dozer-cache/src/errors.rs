use dozer_types::thiserror;
use dozer_types::thiserror::Error;

use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{DeserializationError, SerializationError, TypeError};

#[derive(Error, Debug)]
pub enum CacheError {
    #[error(transparent)]
    Internal(#[from] BoxedError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error(transparent)]
    Storage(#[from] dozer_storage::errors::StorageError),
    #[error("Schema Identifier is not present")]
    SchemaIdentifierNotFound,
    #[error("Path not initialized for Cache Reader")]
    PathNotInitialized,
    #[error("Secondary index database is not found")]
    SecondaryIndexDatabaseNotFound,
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
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Failed to get a record by id - {0:?}")]
    GetValue(#[source] dozer_storage::lmdb::Error),
    #[error("Get by primary key is not supported when it is composite: {0:?}")]
    MultiIndexFetch(String),
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
    #[error(transparent)]
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
    #[error(transparent)]
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
