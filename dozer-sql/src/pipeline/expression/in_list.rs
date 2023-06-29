use dozer_types::types::{Field, Record, Schema};

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

pub(crate) fn evaluate_in_list(
    schema: &Schema,
    expr: &Expression,
    list: &[Expression],
    record: &Record,
) -> Result<Field, PipelineError> {
    let expr_value = expr.evaluate(record, schema)?;
    let mut found = false;
    for item in list {
        let item_value = item.evaluate(record, schema)?;
        if item_value == expr_value {
            found = true;
            break;
        }
    }

    Ok(Field::Boolean(found))
}
