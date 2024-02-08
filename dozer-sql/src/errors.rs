#![allow(clippy::enum_variant_names)]

use dozer_core::checkpoint::serialize::DeserializationError;
use dozer_core::node::PortHandle;
use dozer_types::chrono::RoundingError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;

use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use dozer_types::types::{Field, FieldType};
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
    #[error("Invalid return type: {0}")]
    InvalidReturnType(String),
    #[error("Invalid function: {0}")]
    InvalidFunction(String),
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("Invalid argument for function {0}(): argument: {1}, index: {2}")]
    InvalidFunctionArgument(String, Field, usize),
    #[error("Not enough arguments for function {0}()")]
    NotEnoughArguments(String),
    #[error("Missing INTO clause for top-level SELECT statement")]
    MissingIntoClause,
    #[error("Duplicate INTO table name found: {0:?}")]
    DuplicateIntoClause(String),

    // Error forwarding
    #[error("Internal type error: {0}")]
    InternalTypeError(#[from] TypeError),
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),

    #[error("Expression error: {0}")]
    Expression(#[from] dozer_sql_expression::error::Error),

    #[error("Unsupported sql: {0}")]
    UnsupportedSqlError(#[from] UnsupportedSqlError),

    #[error("Join: {0}")]
    JoinError(#[from] JoinError),

    #[error("Product: {0}")]
    ProductError(#[from] ProductError),

    #[error("Set: {0}")]
    SetError(#[from] SetError),

    #[error("Window: {0}")]
    WindowError(#[from] WindowError),

    #[error("Table Function is not supported")]
    UnsupportedTableFunction,

    #[error("UNNEST not supported")]
    UnsupportedUnnest,

    #[error("Nested Join is not supported")]
    UnsupportedNestedJoin,

    #[error("Pivot is not supported")]
    UnsupportedPivot,

    #[error("Table Operator: {0} is not supported")]
    UnsupportedTableOperator(String),

    #[error("Invalid JOIN: {0}")]
    InvalidJoin(String),

    #[error("The JOIN clause is not supported. In this version only INNER, LEFT and RIGHT OUTER JOINs are supported")]
    UnsupportedJoinType,

    #[error(
        "Unsupported JOIN constraint, only ON is allowed as the JOIN constraint using \'=\' and \'AND\' operators"
    )]
    UnsupportedJoinConstraintType,

    #[error("Unsupported JOIN constraint {0} only comparison of fields with \'=\' and \'AND\' operators are allowed in the JOIN ON constraint")]
    UnsupportedJoinConstraint(String),

    #[error("Invalid JOIN constraint on: {0}")]
    InvalidJoinConstraint(String),

    #[error(
        "Unsupported JOIN constraint operator {0}, only \'=\' and \'AND\' operators are allowed in the JOIN ON constraint"
    )]
    UnsupportedJoinConstraintOperator(String),

    #[error("Currently JOIN supports two level of namespacing. For example, `source.field_name` is valid, but `connection.source.field_name` is not.")]
    NameSpaceTooLong(String),

    #[error("Window: {0}")]
    TableOperatorError(#[from] TableOperatorError),

    #[error("Invalid port handle: {0}")]
    InvalidPortHandle(PortHandle),

    #[error("Duplicated Processor name: {0}")]
    ProcessorAlreadyExists(String),
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
pub enum SetError {
    #[error("Invalid input schemas have been populated")]
    InvalidInputSchemas,
    #[error("Database unavailable for SET")]
    DatabaseUnavailable,
    #[error("History unavailable for SET source [{0}]")]
    HistoryUnavailable(u16),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] DeserializationError),
}

#[derive(Error, Debug)]
pub enum JoinError {
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

    #[error("Overflow error computing the eviction time in the TTL reference field")]
    EvictionTimeOverflow,

    #[error("Field type error computing the eviction time in the TTL reference field")]
    EvictionTypeOverflow,

    #[error("Recordstore error: {0}")]
    RecordStore(#[from] RecordStoreError),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] DeserializationError),
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

    #[error("Error in the FROM clause, Table Function is not supported")]
    UnsupportedTableFunction,

    #[error("Error in the FROM clause, UNNEST is not supported")]
    UnsupportedUnnest,

    #[error("Error in the FROM clause, Pivot is not supported")]
    UnsupportedPivot,
}

#[derive(Error, Debug)]
pub enum WindowError {
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

    #[error("Invalid WINDOW function")]
    InvalidWindow(),

    #[error("For WINDOW functions and alias must be specified")]
    AliasNotSpecified(),

    #[error("Source table not specified in the window function")]
    WindowMissingSourceArgument,

    #[error("Error in the FROM clause, Derived Table is not supported as WINDOW source")]
    UnsupportedDerived,

    #[error("Invalid source table {0} in the window function")]
    WindowInvalidSource(String),

    #[error("WINDOW functions require alias")]
    NoAlias,

    #[error("RecordStore error")]
    RecordStore(#[from] RecordStoreError),
}

#[derive(Error, Debug)]
pub enum TableOperatorError {
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),

    #[error("Source Table not specified in the Table Operator {0}")]
    MissingSourceArgument(String),

    #[error("Invalid source table {0} in the Table Operator {1}")]
    InvalidSourceArgument(String, String),

    #[error("Interval is not specified in the Table Operator {0}")]
    MissingIntervalArgument(String),

    #[error("Invalid time interval '{0}' specified in the Table Operator {1}")]
    InvalidInterval(String, String),

    #[error("Invalid reference expression '{0}' specified in the Table Operator {1}")]
    InvalidReference(String, String),

    #[error("Missing Argument in '{0}' ")]
    MissingArgument(String),

    #[error("TTL input must evaluate to timestamp, but it evaluates to {0}")]
    InvalidTtlInputType(Field),

    #[error("Recordstore error")]
    RecordStore(#[from] RecordStoreError),
}
