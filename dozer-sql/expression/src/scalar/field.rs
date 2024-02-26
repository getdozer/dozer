use crate::error::Error;
use crate::execution::Expression;
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
