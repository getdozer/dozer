mod string;
mod number;

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record};
use num_traits::Float;
use crate::pipeline::expression::scalar::number::{evaluate_abs, evaluate_round};
use crate::pipeline::expression::scalar::string::evaluate_ucase;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
    Ucase,
}

impl ScalarFunctionType {
    pub fn new(name: &str) -> Result<ScalarFunctionType, PipelineError> {
        match name {
            "abs" => Ok(ScalarFunctionType::Abs),
            "round" => Ok(ScalarFunctionType::Round),
            "ucase" => Ok(ScalarFunctionType::Ucase),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        args: &[Expression],
        record: &Record,
    ) -> Result<Field, PipelineError> {
        match self {
            ScalarFunctionType::Abs => evaluate_abs(&args[0], record),
            ScalarFunctionType::Round => evaluate_round(&args[0], args.get(1), record),
            ScalarFunctionType::Ucase => evaluate_ucase(&args[0], record),
        }
    }
}

