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

    let mut contains_null = false;
    for item in list {
        let item = item.evaluate(record, schema)?;
        if item == Field::Null {
            contains_null = true;
            continue;
        }
        if field == item {
            return Ok(Field::Boolean(!negated));
        }
    }

    if contains_null {
        Ok(Field::Null)
    } else {
        Ok(Field::Boolean(negated))
    }
}
