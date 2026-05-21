use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::error::Error;
use crate::execution::Expression;

pub(crate) fn evaluate_in_list(
    schema: &Schema,
    expr: &mut Expression,
    list: &mut [Expression],
    negated: bool,
    record: &Record,
) -> Result<Field, Error> {
    let field = expr.evaluate(record, schema)?;
    if field == Field::Null {
        return Ok(Field::Null);
    }

    let mut result = false;
    let mut saw_null = false;
    for item in list {
        let item = item.evaluate(record, schema)?;
        if item == Field::Null {
            saw_null = true;
            continue;
        }

        if field == item {
            result = true;
            break;
        }
    }
    if !result && saw_null {
        return Ok(Field::Null);
    }

    // Negate the result if the IN list was negated.
    if negated {
        result = !result;
    }
    Ok(Field::Boolean(result))
}
