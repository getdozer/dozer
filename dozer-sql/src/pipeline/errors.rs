#![allow(clippy::enum_variant_names)]

use dozer_core::dag::errors::ExecutionError;
use dozer_core::storage::errors::StorageError;
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
    #[error("{0}() is invoked from another aggregation function. Nesting of aggregation functions is not possible.")]
    InvalidNestedAggregationFunction(String),
    #[error("Field {0} is not present in teh source schema")]
    UnknownFieldIdentifier(String),
    #[error(
        "Field {0} is ambiguous. Specify a fully qualified name such as [connection.]source.field"
    )]
    AmbiguousFieldIdentifier(String),
    #[error("The field identifier {0} is invalid. Correct format is: [[connection.]source.]field")]
    IllegalFieldIdentifier(String),

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
}

#[derive(Error, Debug)]
pub enum UnsupportedSqlError {
    #[error("Recursive CTE is not supported. Please refer to the documentation(https://getdozer.io/docs/reference/sql/introduction) for more information. ")]
    Recursive,
    #[error("Currently this syntax is not supported for CTEs")]
    CteFromError,
    #[error("Currently only SELECT operations are allowed")]
    SelectOnlyError,
    #[error("Unsupported syntax in fROM clause")]
    JoinTable,

    #[error("FROM clause doesn't support \"Comma Syntax\"")]
    FromCommaSyntax,
    #[error("ORDER BY is not supported in SQL. You could achieve the same by using the ORDER BY operator in the cache and APIs")]
    OrderByError,
    #[error("Limit and Offset are not supported in SQL. You could achieve the same by using the LIMIT and OFFSET operators in the cache and APIs")]
    LimitOffsetError,
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
}
