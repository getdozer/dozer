use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{Field, Schema};

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

pub(crate) fn evaluate_in_list(
    schema: &Schema,
    expr: &Expression,
    list: &[Expression],
    negated: bool,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let field = expr.evaluate(record, schema)?;
    let mut result = false;
    for item in list {
        let item = item.evaluate(record, schema)?;
        if field == item {
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
