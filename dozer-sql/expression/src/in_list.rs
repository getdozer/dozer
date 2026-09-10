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
    let mut match_found = false;
    let mut list_contains_null = false;

    if field == Field::Null {
        return Ok(Field::Null);
    }

    for item in list {
        let item = item.evaluate(record, schema)?;
        if item == Field::Null {
            list_contains_null = true;
            continue;
        }

        if field == item {
            match_found = true;
            break;
        }
    }

    let result_field = if match_found {
        Field::Boolean(true)
    } else if list_contains_null {
        // No exact match, but NULL was present in the list -> UNKNOWN
        Field::Null
    } else {
        // No exact match, and no NULLs in the list -> FALSE
        Field::Boolean(false)
    };

    // Negate the result if the IN list was negated.
    if negated {
        match result_field {
            Field::Boolean(b) => Ok(Field::Boolean(!b)),
            Field::Null => Ok(Field::Null), // NOT NULL is NULL
            _ => unreachable!(),
        }
    } else {
        Ok(result_field)
    }
}
