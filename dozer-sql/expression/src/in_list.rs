use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::comparison::evaluate_eq;
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
    let mut result = false;
    for item in list {
        let item = item.evaluate(record, schema)?;

        // Keep the existing NULL behavior while matching normal SQL equality
        // semantics for compatible non-null types (for example Int = Float).
        if matches!(&field, Field::Null) || matches!(&item, Field::Null) {
            if field == item {
                result = true;
                break;
            }
            continue;
        }

        let mut left = Expression::Literal(field.clone());
        let mut right = Expression::Literal(item);
        if evaluate_eq(schema, &mut left, &mut right, record)? == Field::Boolean(true) {
            result = true;
            break;
        }
    }
    // Negate the result if the IN list was negated.
    if negated {
        result = !result;
    }
    Ok(Field::Boolean(result))
}
