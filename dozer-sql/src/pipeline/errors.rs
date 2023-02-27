#![allow(clippy::enum_variant_names)]

use dozer_core::errors::ExecutionError;
use dozer_core::storage::errors::StorageError;
use dozer_types::chrono::RoundingError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{DeserializationError, TypeError};
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use dozer_types::types::{Field, FieldType, Record};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct FieldTypes {
    types: Vec<FieldType>,
}

impl FieldTypes {
    pub fn new(types: Vec<FieldType>) -> Self {
        Self { types }
    }
}

impl Display for FieldTypes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_list: Vec<String> = self.types.iter().map(|e| e.to_string()).collect();
        f.write_str(str_list.join(", ").as_str())
    }
}

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Invalid operand type for function: {0}()")]
    InvalidOperandType(String),
    #[error("Invalid input type. Reason: {0}")]
    InvalidInputType(String),
    #[error("Invalid function: {0}")]
    InvalidFunction(String),
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("Invalid relation")]
    InvalidRelation,
    #[error("Invalid relation")]
    DataTypeMismatch,
    #[error("Invalid argument for function {0}(): argument: {1}, index: {2}")]
    InvalidFunctionArgument(String, Field, usize),
    #[error("Too many arguments for function {0}()")]
    TooManyArguments(String),
    #[error("Not enough arguments for function {0}()")]
    NotEnoughArguments(String),
    #[error(
        "Invalid argument type for function {0}(): type: {1}, expected types: {2}, index: {3}"
    )]
    InvalidFunctionArgumentType(String, FieldType, FieldTypes, usize),
    #[error("Invalid cast: from: {from}, to: {to}")]
    InvalidCast { from: Field, to: FieldType },
    #[error("{0}() cannot be called frome here. Aggregations can only be used in SELECT and HAVING and cannot be nested within other aggregations.")]
    InvalidNestedAggregationFunction(String),
    #[error("Field {0} is not present in the source schema")]
    UnknownFieldIdentifier(String),
    #[error(
        "Field {0} is ambiguous. Specify a fully qualified name such as [connection.]source.field"
    )]
    AmbiguousFieldIdentifier(String),
    #[error("The field identifier {0} is invalid. Correct format is: [[connection.]source.]field")]
    IllegalFieldIdentifier(String),

    #[cfg(feature = "python")]
    #[error("Python Error: {0}")]
    PythonErr(dozer_types::pyo3::PyErr),

    // Error forwarding
    #[error(transparent)]
    InternalStorageError(#[from] StorageError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),

    #[error(transparent)]
    UnsupportedSqlError(#[from] UnsupportedSqlError),

    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error(transparent)]
    SqlError(#[from] SqlError),

    #[error(transparent)]
    SetError(#[from] SetError),
}
#[cfg(feature = "python")]
impl From<dozer_types::pyo3::PyErr> for PipelineError {
    fn from(py_err: dozer_types::pyo3::PyErr) -> Self {
        PipelineError::PythonErr(py_err)
    }
}

#[derive(Error, Debug)]
pub enum UnsupportedSqlError {
    #[error("Recursive CTE is not supported. Please refer to the documentation(https://getdozer.io/docs/reference/sql/introduction) for more information. ")]
    Recursive,
    #[error("Currently this syntax is not supported for CTEs")]
    CteFromError,
    #[error("Currently only SELECT operations are allowed")]
    SelectOnlyError,
    #[error("Unsupported syntax in FROM clause")]
    JoinTable,

    #[error("FROM clause doesn't support \"Comma Syntax\"")]
    FromCommaSyntax,
    #[error("ORDER BY is not supported in SQL. You could achieve the same by using the ORDER BY operator in the cache and APIs")]
    OrderByError,
    #[error("Limit and Offset are not supported in SQL. You could achieve the same by using the LIMIT and OFFSET operators in the cache and APIs")]
    LimitOffsetError,
    #[error("Select statements should specify INTO for creating output tables")]
    IntoError,

    #[error("Unsupported SQL statement {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("SQL Error: The first argument of the {0} function must be a source name.")]
    WindowError(String),
    #[error("SQL Error: Invalid column name {0}.")]
    InvalidColumn(String),
}

#[derive(Error, Debug)]
pub enum SetError {
    #[error("Invalid input schemas have been populated")]
    InvalidInputSchemas,
    #[error("Database unavailable for SET")]
    DatabaseUnavailable,
    #[error("History unavailable for SET source [{0}]")]
    HistoryUnavailable(u16),
    #[error(
        "Record with key: {0:x?} version: {1} not available in History for SET source[{2}]\n{3}"
    )]
    HistoryRecordNotFound(Vec<u8>, u32, u16, dozer_core::errors::ExecutionError),
}

#[derive(Error, Debug)]
pub enum JoinError {
    #[error("Field {0:?} not found")]
    FieldError(String),
    #[error("Currently join supports two level of namespacing. For example, `connection1.field1` is valid, but `connection1.n1.field1` is not.")]
    NameSpaceTooLong(String),
    #[error("Invalid Join constraint on : {0}")]
    InvalidJoinConstraint(String),
    #[error("Ambigous field specified in join : {0}")]
    AmbiguousField(String),
    #[error("Invalid Field specified in join : {0}")]
    InvalidFieldSpecified(String),
    #[error("Unsupported Join constraint {0} only comparison of fields with \'=\' and \'AND\' operators are allowed in the JOIN ON constraint")]
    UnsupportedJoinConstraint(String),
    #[error(
        "Unsupported Join constraint operator {0}, only \'=\' and \'AND\' operators are allowed in the JOIN ON constraint"
    )]
    UnsupportedJoinConstraintOperator(String),
    #[error(
        "Unsupported Join constraint, only ON is allowed as the JOIN constraint using \'=\' and \'AND\' operators"
    )]
    UnsupportedJoinConstraintType,
    #[error("Unsupported Join type")]
    UnsupportedJoinType,
    #[error("Invalid Table name specified")]
    InvalidRelation(String),

    #[error("Invalid Join Source: {0}")]
    InvalidSource(u16),

    #[error("Invalid Key for the record:\n{0}\n{1}")]
    InvalidKey(Record, TypeError),

    #[error("Error trying to deserialise a record from JOIN processor index: {0}")]
    DeserializationError(DeserializationError),

    #[error("History unavailable for JOIN source [{0}]")]
    HistoryUnavailable(u16),

    #[error(
        "Record with key: {0:x?} version: {1} not available in History for JOIN source[{2}]\n{3}"
    )]
    HistoryRecordNotFound(Vec<u8>, u32, u16, dozer_core::errors::ExecutionError),

    #[error("Error inserting key: {0:x?} value: {1:x?} in the JOIN index\n{2}")]
    IndexPutError(Vec<u8>, Vec<u8>, StorageError),

    #[error("Error deleting key: {0:x?} value: {1:x?} from the JOIN index\n{2}")]
    IndexDelError(Vec<u8>, Vec<u8>, StorageError),

    #[error("Error reading key: {0:x?} from the JOIN index\n{1}")]
    IndexGetError(Vec<u8>, StorageError),

    #[error("Error in the FROM clause, Invalid function {0:x?}")]
    UnsupportedRelationFunction(String),

    #[error("Column name not specified in the window function")]
    WindowMissingColumnArgument,

    #[error("Interval not specified in the window function")]
    WindowMissingIntervalArgument,

    #[error("Hop size not specified in the window function")]
    WindowMissingHopSizeArgument,

    #[error("Invalid time reference column {0} in the window function")]
    WindowInvalidColumn(String),

    #[error("Invalid time interval '{0}' specified in the window function")]
    WindowInvalidInterval(String),

    #[error("Invalid time hop '{0}' specified in the window function")]
    WindowInvalidHop(String),

    #[error("Error in the FROM clause, Derived Table is not supported")]
    UnsupportedDerivedTable,

    #[error("Error in the FROM clause, Table Function is not supported")]
    UnsupportedTableFunction,

    #[error("Error in the FROM clause, UNNEST is not supported")]
    UnsupportedUnnest,

    #[error("This type of Nested Join is not supported")]
    UnsupportedNestedJoin,
}

#[derive(Error, Debug)]
pub enum ProductError {
    #[error("Product Processor Database is not initialised properly")]
    InvalidDatabase(),

    #[error("Error deleting a record coming from {0}\n{1}")]
    DeleteError(String, #[source] BoxedError),

    #[error("Error inserting a record coming from {0}\n{1}")]
    InsertError(String, #[source] BoxedError),

    #[error("Error updating a record from {0} cannot delete the old entry\n{1}")]
    UpdateOldError(String, #[source] BoxedError),

    #[error("Error updating a record from {0} cannot insert the new entry\n{1}")]
    UpdateNewError(String, #[source] BoxedError),
}

#[derive(Error, Debug)]
pub enum WindowError {
    #[error("Invalid column specified in Tumble Windowing function.\nOnly Timestamp and Date types are supported")]
    TumbleInvalidColumnType(),
    #[error("Invalid column specified in Tumble Windowing function.")]
    TumbleInvalidColumnIndex(),
    #[error("Error in Tumble Windowing function:\n{0}")]
    TumbleRoundingError(#[source] RoundingError),

    #[error("Invalid column specified in Hop Windowing function.\nOnly Timestamp and Date types are supported")]
    HopInvalidColumnType(),
    #[error("Invalid column specified in Hop Windowing function.")]
    HopInvalidColumnIndex(),
    #[error("Error in Hop Windowing function:\n{0}")]
    HopRoundingError(#[source] RoundingError),
}
