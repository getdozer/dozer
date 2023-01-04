mod number;
mod string;
mod tests;

use crate::argv;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::number::{evaluate_abs, evaluate_round};
use crate::pipeline::expression::scalar::string::{
    evaluate_concat, evaluate_length, evaluate_trim, evaluate_ucase,
};

use dozer_types::types::{Field, FieldType, Record, Schema};

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
    Ucase,
    Concat,
    Length,
    Trim,
}

impl Display for ScalarFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarFunctionType::Abs => f.write_str("ABS"),
            ScalarFunctionType::Round => f.write_str("ROUND"),
            ScalarFunctionType::Ucase => f.write_str("UCASE"),
            ScalarFunctionType::Concat => f.write_str("CONCAT"),
            ScalarFunctionType::Length => f.write_str("LENGTH"),
            ScalarFunctionType::Trim => f.write_str("TRIM_MATCH"),
        }
    }
}

pub(crate) fn get_scalar_function_type(
    function: &ScalarFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<FieldType, PipelineError> {
    match function {
        ScalarFunctionType::Abs => argv!(args, 0, ScalarFunctionType::Abs)?.get_type(schema),
        ScalarFunctionType::Round => Ok(FieldType::Int),
        ScalarFunctionType::Ucase => argv!(args, 0, ScalarFunctionType::Ucase)?.get_type(schema),
        ScalarFunctionType::Concat => Ok(FieldType::String),
        ScalarFunctionType::Length => Ok(FieldType::UInt),
        ScalarFunctionType::Trim => Ok(FieldType::String),
    }
}

impl ScalarFunctionType {
    pub fn new(name: &str) -> Result<ScalarFunctionType, PipelineError> {
        match name {
            "abs" => Ok(ScalarFunctionType::Abs),
            "round" => Ok(ScalarFunctionType::Round),
            "ucase" => Ok(ScalarFunctionType::Ucase),
            "concat" => Ok(ScalarFunctionType::Concat),
            "length" => Ok(ScalarFunctionType::Length),
            "trim_match" => Ok(ScalarFunctionType::Trim),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        args: &[Expression],
        record: &Record,
    ) -> Result<Field, PipelineError> {
        match self {
            ScalarFunctionType::Abs => {
                evaluate_abs(argv!(args, 0, ScalarFunctionType::Abs)?, record)
            }
            ScalarFunctionType::Round => evaluate_round(
                argv!(args, 0, ScalarFunctionType::Round)?,
                args.get(1),
                record,
            ),
            ScalarFunctionType::Ucase => {
                evaluate_ucase(argv!(args, 0, ScalarFunctionType::Ucase)?, record)
            }
            ScalarFunctionType::Concat => evaluate_concat(
                argv!(args, 0, ScalarFunctionType::Concat)?,
                argv!(args, 1, ScalarFunctionType::Concat)?,
                record,
            ),
            ScalarFunctionType::Length => {
                evaluate_length(argv!(args, 0, ScalarFunctionType::Length)?, record)
            }
            ScalarFunctionType::Trim => evaluate_trim(
                argv!(args, 0, ScalarFunctionType::Trim)?,
                args.get(1),
                record,
            ),
        }
    }
}
