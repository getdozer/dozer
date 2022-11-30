use crate::sources::subscriptions::RelationUniqueName;
use dozer_types::types::{FieldDefinition, Schema};

pub struct ExecutionFieldDef {
    pub relation: RelationUniqueName,
    pub aliases: Vec<String>,
    pub def: FieldDefinition,
}

impl ExecutionFieldDef {
    pub fn new(relation: RelationUniqueName, def: FieldDefinition) -> Self {
        Self {
            relation,
            aliases: Vec::new(),
            def,
        }
    }
}

pub struct ExecutionSchema {
    pub fields: Vec<ExecutionFieldDef>,
    pub primary_key: Vec<usize>,
}

impl ExecutionSchema {
    pub fn from_schema(name: RelationUniqueName, schema: &Schema) -> ExecutionSchema {
        let fields: Vec<ExecutionFieldDef> = schema
            .fields
            .iter()
            .map(|f| ExecutionFieldDef::new(name.clone(), f.clone()))
            .collect();

        ExecutionSchema::new(fields, schema.primary_index.clone())
    }

    fn new(fields: Vec<ExecutionFieldDef>, primary_key: Vec<usize>) -> Self {
        Self {
            fields,
            primary_key,
        }
    }
}
