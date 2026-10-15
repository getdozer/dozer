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
    let mut left_literal = Expression::Literal(field);
    let mut result = false;
    for item in list {
        if evaluate_eq(schema, &mut left_literal, item, record)? == Field::Boolean(true) {
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
