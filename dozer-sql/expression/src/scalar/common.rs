use crate::arg_utils::{validate_num_arguments, validate_one_argument, validate_two_arguments};
use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use crate::scalar::number::{evaluate_abs, evaluate_round};
use crate::scalar::string::{
    evaluate_concat, evaluate_length, evaluate_to_char, evaluate_ucase, validate_concat,
    validate_ucase,
};
use dozer_types::types::Record;
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
) -> Result<ExpressionType, Error> {
    match function {
        ScalarFunctionType::Abs => validate_one_argument(args, schema, ScalarFunctionType::Abs),
        ScalarFunctionType::Round => {
            let return_type = if args.len() == 1 {
                validate_one_argument(args, schema, ScalarFunctionType::Round)?.return_type
            } else {
                validate_two_arguments(args, schema, ScalarFunctionType::Round)?
                    .0
                    .return_type
            };
            Ok(ExpressionType::new(
                return_type,
                true,
                dozer_types::types::SourceDefinition::Dynamic,
                false,
            ))
        }
        ScalarFunctionType::Ucase => {
            validate_num_arguments(1..2, args.len(), ScalarFunctionType::Ucase)?;
            validate_ucase(&args[0], schema)
        }
        ScalarFunctionType::Concat => validate_concat(args, schema),
        ScalarFunctionType::Length => Ok(ExpressionType::new(
            FieldType::UInt,
            false,
            dozer_types::types::SourceDefinition::Dynamic,
            false,
        )),
        ScalarFunctionType::ToChar => {
            if args.len() == 1 {
                validate_one_argument(args, schema, ScalarFunctionType::ToChar)
            } else {
                Ok(validate_two_arguments(args, schema, ScalarFunctionType::ToChar)?.0)
            }
        }
    }
}

impl ScalarFunctionType {
    pub fn new(name: &str) -> Option<ScalarFunctionType> {
        match name {
            "abs" => Some(ScalarFunctionType::Abs),
            "round" => Some(ScalarFunctionType::Round),
            "ucase" => Some(ScalarFunctionType::Ucase),
            "concat" => Some(ScalarFunctionType::Concat),
            "length" => Some(ScalarFunctionType::Length),
            "to_char" => Some(ScalarFunctionType::ToChar),
            _ => None,
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &mut [Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        match self {
            ScalarFunctionType::Abs => {
                validate_num_arguments(1..2, args.len(), ScalarFunctionType::Abs)?;
                evaluate_abs(schema, &mut args[0], record)
            }
            ScalarFunctionType::Round => {
                validate_num_arguments(1..3, args.len(), ScalarFunctionType::Round)?;
                let (arg0, arg1) = args.split_at_mut(1);
                evaluate_round(schema, &mut arg0[0], arg1.get_mut(0), record)
            }
            ScalarFunctionType::Ucase => {
                validate_num_arguments(1..2, args.len(), ScalarFunctionType::Ucase)?;
                evaluate_ucase(schema, &mut args[0], record)
            }
            ScalarFunctionType::Concat => evaluate_concat(schema, args, record),
            ScalarFunctionType::Length => {
                validate_num_arguments(1..2, args.len(), ScalarFunctionType::Length)?;
                evaluate_length(schema, &mut args[0], record)
            }
            ScalarFunctionType::ToChar => {
                validate_num_arguments(2..3, args.len(), ScalarFunctionType::ToChar)?;
                let (arg0, arg1) = args.split_at_mut(1);
                evaluate_to_char(schema, &mut arg0[0], &mut arg1[0], record)
            }
        }
    }
}
