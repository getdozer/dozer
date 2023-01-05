use crate::arg_str;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::arg_utils::validate_arg_type;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, TrimType};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use dozer_types::types::{Field, FieldType, Record, Schema};

pub(crate) fn validate_trim(arg: &Expression, schema: &Schema) -> Result<FieldType, PipelineError> {
    let ret_type = validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;
    Ok(ret_type)
}

pub(crate) fn evaluate_trim(
    arg: &Box<Expression>,
    what: &Option<Box<Expression>>,
    typ: &Option<TrimType>,
    record: &Record,
) -> Result<Field, PipelineError> {
    let arg_field = arg.evaluate(record)?;
    let arg_value = arg_str!(arg_field, "TRIM", 0)?;

    let v1: Vec<_> = match what {
        Some(e) => {
            let f = e.evaluate(record)?;
            arg_str!(f, "TRIM", 1)?.chars().collect()
        }
        _ => vec![' '],
    };

    let retval = match typ {
        Some(TrimType::Both) => arg_value.trim_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Leading) => arg_value.trim_left_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Trailing) => arg_value.trim_right_matches::<&[char]>(&v1).to_string(),
        None => arg_value.trim_matches::<&[char]>(&v1).to_string(),
    };

    Ok(Field::String(retval))
}
