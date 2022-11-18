#![allow(clippy::enum_variant_names)]

use dozer_types::serde_json::Value;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{DeserializationError, SerializationError, TypeError};

#[derive(Error, Debug)]
pub enum CacheError {
    #[error(transparent)]
    QueryValidationError(#[from] QueryValidationError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    QueryError(#[from] QueryError),
    #[error(transparent)]
    IndexError(#[from] IndexError),
    #[error(transparent)]
    PlanError(#[from] PlanError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Schema Identifier is not present")]
    SchemaIdentifierNotFound,
    #[error("Path not initialized for Cache Reader")]
    PathNotInitialized,
}

impl CacheError {
    pub fn map_serialization_error(e: dozer_types::bincode::Error) -> CacheError {
        CacheError::TypeError(TypeError::SerializationError(SerializationError::Bincode(
            e,
        )))
    }
    pub fn map_deserialization_error(e: dozer_types::bincode::Error) -> CacheError {
        CacheError::TypeError(TypeError::DeserializationError(
            DeserializationError::Bincode(e),
        ))
    }
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Failed to get value")]
    GetValue(#[source] lmdb::Error),
    #[error("Failed to insert value")]
    InsertValue(#[source] lmdb::Error),
    #[error("Field not found")]
    FieldNotFound,
    #[error("Cannot access record")]
    AccessDenied,
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
pub enum QueryValidationError {
    #[error("Scalar value cannot contain special character")]
    SpecialCharacterError,
    #[error("empty object passed as value")]
    EmptyObjectAsValue,
    #[error("empty array passed as value")]
    EmptyArrayAsValue,

    #[error("unexpected character : {0}")]
    UnexpectedCharacter(String),

    #[error("unexpected object: {0}")]
    UnexpectedObject(Value),

    #[error("unidentified operator {0}")]
    UnidentifiedOperator(String),

    #[error("More than one statement passed in Simple Expression")]
    MoreThanOneStmt,

    #[error("Invalid Expression")]
    InvalidExpression,

    #[error("Invalid Expression")]
    InvalidAndExpression,
}

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("Field not found")]
    FieldNotFound,
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

pub fn validate_query(
    condition: bool,
    err: QueryValidationError,
) -> Result<(), QueryValidationError> {
    if !condition {
        Err(err)
    } else {
        Ok(())
    }
}
