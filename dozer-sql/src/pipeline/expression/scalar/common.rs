use crate::argv;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::pipeline::expression::scalar::number::{evaluate_abs, evaluate_round};
use crate::pipeline::expression::scalar::string::{
    evaluate_concat, evaluate_length, evaluate_to_char, evaluate_ucase, validate_concat,
    validate_ucase,
};
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{Field, FieldType, Schema};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
    Ucase,
    Concat,
    Length,
    ToChar,
}

impl Display for ScalarFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarFunctionType::Abs => f.write_str("ABS"),
            ScalarFunctionType::Round => f.write_str("ROUND"),
            ScalarFunctionType::Ucase => f.write_str("UCASE"),
            ScalarFunctionType::Concat => f.write_str("CONCAT"),
            ScalarFunctionType::Length => f.write_str("LENGTH"),
            ScalarFunctionType::ToChar => f.write_str("TO_CHAR"),
        }
    }
}

pub(crate) fn get_scalar_function_type(
    function: &ScalarFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    match function {
        ScalarFunctionType::Abs => argv!(args, 0, ScalarFunctionType::Abs)?.get_type(schema),
        ScalarFunctionType::Round => {
            let return_type = argv!(args, 0, ScalarFunctionType::Round)?
                .get_type(schema)?
                .return_type;
            Ok(ExpressionType::new(
                return_type,
                true,
                dozer_types::types::SourceDefinition::Dynamic,
                false,
            ))
        }
        ScalarFunctionType::Ucase => {
            validate_ucase(argv!(args, 0, ScalarFunctionType::Ucase)?, schema)
        }
        ScalarFunctionType::Concat => validate_concat(args, schema),
        ScalarFunctionType::Length => Ok(ExpressionType::new(
            FieldType::UInt,
            false,
            dozer_types::types::SourceDefinition::Dynamic,
            false,
        )),
        ScalarFunctionType::ToChar => argv!(args, 0, ScalarFunctionType::ToChar)?.get_type(schema),
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
            "to_char" => Ok(ScalarFunctionType::ToChar),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &[Expression],
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        match self {
            ScalarFunctionType::Abs => {
                evaluate_abs(schema, argv!(args, 0, ScalarFunctionType::Abs)?, record)
            }
            ScalarFunctionType::Round => evaluate_round(
                schema,
                argv!(args, 0, ScalarFunctionType::Round)?,
                args.get(1),
                record,
            ),
            ScalarFunctionType::Ucase => {
                evaluate_ucase(schema, argv!(args, 0, ScalarFunctionType::Ucase)?, record)
            }
            ScalarFunctionType::Concat => evaluate_concat(schema, args, record),
            ScalarFunctionType::Length => {
                evaluate_length(schema, argv!(args, 0, ScalarFunctionType::Length)?, record)
            }
            ScalarFunctionType::ToChar => evaluate_to_char(
                schema,
                argv!(args, 0, ScalarFunctionType::ToChar)?,
                argv!(args, 1, ScalarFunctionType::ToChar)?,
                record,
            ),
        }
    }
}
