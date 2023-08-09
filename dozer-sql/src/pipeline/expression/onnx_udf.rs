use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::UnsupportedSqlError;
use crate::pipeline::errors::UnsupportedSqlError::GenericError;
use crate::pipeline::expression::execution::Expression;
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, Schema};
use std::env;
use std::path::PathBuf;

const MODULE_NAME: &str = "onnx_udf";

pub fn evaluate_onnx_udf(
    schema: &Schema,
    name: &str,
    args: &[Expression],
    return_type: &FieldType,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    todo!();
}
