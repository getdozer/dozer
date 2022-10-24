use std::collections::HashMap;

use dozer_types::{
    errors::{
        pipeline::PipelineError, pipeline::PipelineError::InternalTypeError, types::TypeError,
    },
    types::Schema,
};

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
