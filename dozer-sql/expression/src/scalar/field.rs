use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

pub(crate) fn evaluate_nvl(
    schema: &Schema,
    arg: &mut Expression,
    replacement: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;
    let replacement_field = replacement.evaluate(record, schema)?;
    if replacement_field.as_string().is_some() && arg_field == Field::Null {
        Ok(replacement_field)
    } else {
        Ok(arg_field)
    }
}

pub fn validate_decode(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    if args.len() < 3 {
        return Err(Error::InvalidNumberOfArguments {
            function_name: "decode".to_string(),
            expected: 3..usize::MAX,
            actual: args.len(),
        });
    }

    let ret_type = args[2].get_type(schema)?.return_type;

    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_decode(
    schema: &Schema,
    arg: &mut Expression,
    results: &mut [Expression],
    default: Option<Expression>,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;
    let mut i = 0;
    while i < results.len() {
        let result = &mut results[i];
        let result_field = result.evaluate(record, schema)?;
        if arg_field == result_field {
            return Ok(results[i + 1].evaluate(record, schema)?);
        }
        i += 2;
    }
    if let Some(mut default) = default {
        default.evaluate(record, schema)
    } else {
        Ok(Field::Null)
    }
}
