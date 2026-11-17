use crate::error::Error;
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition, FieldType};

#[derive(Clone, Debug, PartialEq)]
pub struct SubqueryPlan {
    pub output_schema: Schema,
}

impl SubqueryPlan {
    pub fn new(fields: Vec<FieldDefinition>) -> Self {
        let mut schema = Schema::new();
        for field_def in fields {
            schema.field(field_def, false);
        }
        Self { output_schema: schema }
    }
    pub fn empty() -> Self {
        Self { output_schema: Schema::new() }
    }
}
