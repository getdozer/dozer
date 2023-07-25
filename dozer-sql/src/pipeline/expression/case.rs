use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::types::{Field, Schema};
use std::iter::zip;

pub fn evaluate_case(
    schema: &Schema,
    _operand: &Option<Box<Expression>>,
    conditions: &Vec<Expression>,
    results: &Vec<Expression>,
    else_result: &Option<Box<Expression>>,
    record_store: &ProcessorRecordStore,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let iter = zip(conditions, results);
    for (cond, res) in iter {
        let field = cond.evaluate(record_store, record, schema)?;
        if let Some(cond_match) = field.as_boolean() {
            if cond_match {
                let then_res = res.evaluate(record_store, record, schema)?;
                return Ok(then_res);
            } else {
                continue;
            }
        }
    }
    if let Some(else_res) = else_result {
        let else_return = else_res.evaluate(record_store, record, schema)?;
        Ok(else_return)
    } else {
        Ok(Field::Null)
    }
}
