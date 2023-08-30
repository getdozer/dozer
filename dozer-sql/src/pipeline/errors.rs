#![allow(clippy::enum_variant_names)]

use dozer_core::node::PortHandle;
use dozer_storage::errors::StorageError;
use dozer_types::chrono::RoundingError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
#[cfg(feature = "onnx")]
use ort::OrtError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use dozer_types::types::{Field, FieldType};
#[cfg(feature = "onnx")]
use ndarray::ShapeError;
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
    #[error("Invalid return type: {0}")]
    InvalidReturnType(String),
    #[error("Invalid function: {0}")]
    InvalidFunction(String),
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Invalid types on {0} and {1} for {2} operand")]
    InvalidTypeComparison(Field, Field, String),
    #[error("Invalid types on {0} for {1} operand")]
    InvalidType(Field, String),
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
    #[error("Mismatching argument types for {0}(): {1}, consider using CAST function")]
    InvalidConditionalExpression(String, FieldTypes),
    #[error("Invalid cast: from: {from}, to: {to}")]
    InvalidCast { from: Field, to: FieldType },
    #[error("{0}() cannot be called from here. Aggregations can only be used in SELECT and HAVING and cannot be nested within other aggregations.")]
    InvalidNestedAggregationFunction(String),
    #[error("Field {0} is not present in the source schema")]
    UnknownFieldIdentifier(String),
    #[error(
        "Field {0} is ambiguous. Specify a fully qualified name such as [connection.]source.field"
    )]
    AmbiguousFieldIdentifier(String),
    #[error("The field identifier {0} is invalid. Correct format is: [[connection.]source.]field")]
    IllegalFieldIdentifier(String),
    #[error("Unable to cast {0} to {1}")]
    UnableToCast(String, String),
    #[error("Missing INTO clause for top-level SELECT statement")]
    MissingIntoClause,
    #[cfg(feature = "python")]
    #[error("Python Error: {0}")]
    PythonErr(dozer_types::pyo3::PyErr),
    #[cfg(feature = "onnx")]
    #[error("Onnx Ndarray Error: {0}")]
    OnnxShapeErr(ShapeError),
    #[cfg(feature = "onnx")]
    #[error("Onnx Runtime Error: {0}")]
    OnnxOrtErr(OrtError),
    #[cfg(feature = "onnx")]
    #[error("Onnx Validation Error: {0}")]
    OnnxValidationErr(String),

    #[error("Udf is defined but missing with config: {0}")]
    UdfConfigMissing(String),

    // Error forwarding
    #[error("Internal type error: {0}")]
    InternalTypeError(#[from] TypeError),
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),

    #[error("Unsupported sql: {0}")]
    UnsupportedSqlError(#[from] UnsupportedSqlError),

    #[error("Join: {0}")]
    JoinError(#[from] JoinError),

    #[error("Product: {0}")]
    ProductError(#[from] ProductError),

    #[error("Set: {0}")]
    SetError(#[from] SetError),

    #[error("Sql: {0}")]
    SqlError(#[from] SqlError),

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

    #[error("Invalid Field specified in JOIN: {0}")]
    InvalidFieldSpecified(String),

    #[error("Currently JOIN supports two level of namespacing. For example, `source.field_name` is valid, but `connection.source.field_name` is not.")]
    NameSpaceTooLong(String),

    #[error("Error building the JOIN on the {0} source of the Processor")]
    JoinBuild(String),

    #[error("Window: {0}")]
    TableOperatorError(#[from] TableOperatorError),

    #[error("Invalid port handle: {0}")]
    InvalidPortHandle(PortHandle),
    #[error("JOIN processor received a Record from a wrong input: {0}")]
    InvalidPort(u16),
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
    #[error(transparent)]
    Operation(#[from] OperationError),
}

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("SQL Error: Addition operation cannot be done due to overflow.")]
    AdditionOverflow,
    #[error("SQL Error: Subtraction operation cannot be done due to overflow.")]
    SubtractionOverflow,
    #[error("SQL Error: Multiplication operation cannot be done due to overflow.")]
    MultiplicationOverflow,
    #[error("SQL Error: Division operation cannot be done.")]
    DivisionByZeroOrOverflow,
    #[error("SQL Error: Modulo operation cannot be done.")]
    ModuloByZeroOrOverflow,
}

#[derive(Error, Debug)]
pub enum SetError {
    #[error("Invalid input schemas have been populated")]
    InvalidInputSchemas,
    #[error("Database unavailable for SET")]
    DatabaseUnavailable,
    #[error("History unavailable for SET source [{0}]")]
    HistoryUnavailable(u16),
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

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
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

    #[error("Storage error")]
    Storage(#[from] StorageError),
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

    #[error("Storage error")]
    Storage(#[from] StorageError),
}
