use std::ops::Range;

use dozer_types::{
    thiserror::{self, Error},
    types::{Field, FieldType},
};
use sqlparser::ast::{
    BinaryOperator, DataType, DateTimeField, Expr, FunctionArg, Ident, UnaryOperator,
};

use crate::{aggregate::AggregateFunctionType, operator::BinaryOperatorType};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unsupported SQL expression: {0:?}")]
    UnsupportedExpression(Expr),
    #[error("Unsupported SQL function arg: {0:?}")]
    UnsupportedFunctionArg(FunctionArg),
    #[error("Invalid ident: {}", .0.iter().map(|ident| ident.value.as_str()).collect::<Vec<_>>().join("."))]
    InvalidIdent(Vec<Ident>),
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Missing leading field in interval")]
    MissingLeadingFieldInInterval,
    #[error("Unsupported SQL unary operator: {0:?}")]
    UnsupportedUnaryOperator(UnaryOperator),
    #[error("Unsupported SQL binary operator: {0:?}")]
    UnsupportedBinaryOperator(BinaryOperator),
    #[error("Not a number: {0}")]
    NotANumber(String),
    #[error("Unsupported data type: {0}")]
    UnsupportedDataType(DataType),

    #[error("Aggregate Function {0:?} should not be executed at this point")]
    UnexpectedAggregationExecution(AggregateFunctionType),
    #[error("literal expression cannot be null")]
    LiteralExpressionIsNull,
    #[error("cannot apply NOT to {0:?}")]
    CannotApplyNotTo(FieldType),
    #[error("cannot apply {operator:?} to {left_field_type:?} and {right_field_type:?}")]
    CannotApplyBinaryOperator {
        operator: BinaryOperatorType,
        left_field_type: FieldType,
        right_field_type: FieldType,
    },
    #[error("expected {expected:?} arguments for function {function_name}, got {actual}")]
    InvalidNumberOfArguments {
        function_name: String,
        expected: Range<usize>,
        actual: usize,
    },
    #[error("Empty coalesce arguments")]
    EmptyCoalesceArguments,
    #[error(
        "Invalid argument type for function {function_name}: type: {actual}, expected types: {expected:?}, index: {argument_index}"
    )]
    InvalidFunctionArgumentType {
        function_name: String,
        argument_index: usize,
        expected: Vec<FieldType>,
        actual: FieldType,
    },
    #[error("Invalid cast: from: {from}, to: {to}")]
    InvalidCast { from: Field, to: FieldType },
    #[error("Invalid argument for function {function_name}(): argument: {argument}, index: {argument_index}")]
    InvalidFunctionArgument {
        function_name: String,
        argument_index: usize,
        argument: Field,
    },

    #[error("Invalid distance algorithm: {0}")]
    InvalidDistanceAlgorithm(String),
    #[error("Failed to calculate vincenty distance: {0}")]
    FailedToCalculateVincentyDistance(
        #[from] dozer_types::geo::vincenty_distance::FailedToConvergeError,
    ),

    #[error("Invalid like escape: {0}")]
    InvalidLikeEscape(#[from] like::InvalidEscapeError),
    #[error("Invalid like pattern: {0}")]
    InvalidLikePattern(#[from] like::InvalidPatternError),

    #[error("Unsupported extract: {0}")]
    UnsupportedExtract(DateTimeField),
    #[error("Unsupported interval: {0}")]
    UnsupportedInterval(DateTimeField),

    #[error("Invalid json path: {0}")]
    InvalidJsonPath(String),

    #[cfg(feature = "python")]
    #[error("Python UDF error: {0}")]
    PythonUdf(#[from] crate::python_udf::Error),

    #[cfg(feature = "onnx")]
    #[error("ONNX UDF error: {0}")]
    Onnx(#[from] crate::onnx::error::Error),
    #[cfg(not(feature = "onnx"))]
    #[error("ONNX UDF is not enabled")]
    OnnxNotEnabled,

    #[error("JavaScript UDF error: {0}")]
    JavaScript(#[from] crate::javascript::Error),

    #[cfg(feature = "wasm")]
    #[error("WASM UDF error: {0}")]
    Wasm(#[from] crate::wasm_udf::WasmError),
    #[cfg(not(feature = "wasm"))]
    #[error("WASM UDF is not enabled here")]
    WasmNotEnabled,

    #[error("Unsupported UDF type")]
    UnsupportedUdfType,

    // Legacy error types.
    #[error("Sql error: {0}")]
    SqlError(#[source] OperationError),
    #[error("Invalid types on {0} and {1} for {2} operand")]
    InvalidTypeComparison(Field, Field, String),
    #[error("Unable to cast {0} to {1}")]
    UnableToCast(String, String),
    #[error("Invalid types on {0} for {1} operand")]
    InvalidType(Field, String),
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
