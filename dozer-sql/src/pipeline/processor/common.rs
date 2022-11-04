use std::collections::HashMap;

use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InternalTypeError;
use dozer_types::{errors::types::TypeError, types::Schema};

pub fn column_index(name: &String, schema: &Schema) -> Result<usize, PipelineError> {
    let schema_idx: HashMap<String, usize> = schema
        .fields
        .iter()
        .enumerate()
        .map(|e| (e.1.name.clone(), e.0))
        .collect();

    if let Some(index) = schema_idx.get(name).cloned() {
        Ok(index)
    } else {
        Err(InternalTypeError(TypeError::InvalidFieldName(name.clone())))
    }
}
