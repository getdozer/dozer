use crate::arg_str;
use crate::pipeline::builder::get_select;
use crate::pipeline::errors::PipelineError;
#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::{evaluate_round, ScalarFunctionType};
use crate::pipeline::projection::factory::ProjectionProcessorFactory;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::ProcessorFactory;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub(crate) fn evaluate_ucase(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::String(s) => Ok(Field::String(s.to_uppercase())),
        Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
        _ => Err(PipelineError::InvalidFunctionArgument(
            ScalarFunctionType::Ucase.to_string(),
            value,
            0,
        )),
    }
}

pub(crate) fn evaluate_concat(
    arg0: &Expression,
    arg1: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let (f0, f1) = (arg0.evaluate(record)?, arg1.evaluate(record)?);
    let (v0, v1) = (
        arg_str!(f0, ScalarFunctionType::Concat, 0)?,
        arg_str!(f1, ScalarFunctionType::Concat, 1)?,
    );
    Ok(Field::String(v0.to_owned() + v1))
}

pub(crate) fn evaluate_length(arg0: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let f0 = arg0.evaluate(record)?;
    let v0 = arg_str!(f0, ScalarFunctionType::Concat, 0)?;
    Ok(Field::UInt(v0.len() as u64))
}

pub(crate) fn evaluate_trim(
    arg0: &Expression,
    arg1: Option<&Expression>,
    record: &Record,
) -> Result<Field, PipelineError> {
    let f0 = arg0.evaluate(record)?;
    let v0 = arg_str!(f0, ScalarFunctionType::Trim, 0)?;

    let v1: Vec<_> = match arg1 {
        Some(e) => {
            let f = e.evaluate(record)?;
            arg_str!(f, ScalarFunctionType::Trim, 1)?.chars().collect()
        }
        _ => vec![' '],
    };

    Ok(Field::String(v0.trim_matches::<&[char]>(&v1).to_string()))
}
