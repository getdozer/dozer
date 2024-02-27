use crate::arg_utils::{validate_num_arguments, validate_one_argument, validate_two_arguments};
use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use crate::scalar::field::evaluate_nvl;
use crate::scalar::number::{evaluate_abs, evaluate_round};
use crate::scalar::string::{
    evaluate_concat, evaluate_length, evaluate_to_char, evaluate_ucase, validate_concat,
    validate_ucase,
};
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};
use std::fmt::{Display, Formatter};

use super::field::{evaluate_decode, validate_decode};
use super::string::{
    evaluate_chr, evaluate_replace, evaluate_substr, validate_replace, validate_substr,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
    Ucase,
    Concat,
    Length,
    ToChar,
    Chr,
    Substr,
    Nvl,
    Replace,
    Decode,
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
            ScalarFunctionType::Chr => f.write_str("CHR"),
            ScalarFunctionType::Substr => f.write_str("SUBSTR"),
            ScalarFunctionType::Nvl => f.write_str("NVL"),
            ScalarFunctionType::Replace => f.write_str("REPLACE"),
            ScalarFunctionType::Decode => f.write_str("DECODE"),
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
        ScalarFunctionType::Chr => validate_one_argument(args, schema, ScalarFunctionType::Chr),
        ScalarFunctionType::Substr => validate_substr(args, schema),
        ScalarFunctionType::Nvl => {
            Ok(validate_two_arguments(args, schema, ScalarFunctionType::Nvl)?.0)
        }
        ScalarFunctionType::Replace => validate_replace(args, schema),
        ScalarFunctionType::Decode => validate_decode(args, schema),
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
            "chr" => Some(ScalarFunctionType::Chr),
            "substr" => Some(ScalarFunctionType::Substr),
            "replace" => Some(ScalarFunctionType::Replace),
            "nvl" => Some(ScalarFunctionType::Nvl),
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
            ScalarFunctionType::Chr => {
                validate_one_argument(args, schema, ScalarFunctionType::Chr)?;
                evaluate_chr(schema, &mut args[0], record)
            }
            ScalarFunctionType::Substr => {
                validate_num_arguments(2..3, args.len(), ScalarFunctionType::Substr)?;
                let mut arg2 = args.get(2).map(|arg| Box::new(arg.clone()));
                evaluate_substr(
                    schema,
                    &mut args[0].clone(),
                    &mut args[1].clone(),
                    &mut arg2,
                    record,
                )
            }
            ScalarFunctionType::Nvl => {
                validate_two_arguments(args, schema, ScalarFunctionType::Nvl)?;
                evaluate_nvl(schema, &mut args[0].clone(), &mut args[1].clone(), record)
            }
            ScalarFunctionType::Replace => {
                validate_replace(args, schema)?;
                evaluate_replace(
                    schema,
                    &mut args[0].clone(),
                    &mut args[1].clone(),
                    &mut args[2].clone(),
                    record,
                )
            }
            ScalarFunctionType::Decode => {
                validate_decode(args, schema)?;

                let (arg0, results) = args.split_at_mut(1);
                let (results, default) = if results.len() % 2 == 0 {
                    results.split_at_mut(results.len() - 1)
                } else {
                    results.split_at_mut(results.len())
                };

                let default = if default.len() == 0 {
                    None
                } else {
                    Some(default[0].clone())
                };

                evaluate_decode(schema, &mut arg0[0], results, default, record)
            }
        }
    }
}
