use std::sync::Arc;

use dozer_types::types::{FieldDefinition, IndexDefinition, IndexType, Schema, SchemaIdentifier};

pub fn get_schema() -> Schema {
    let schema = Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![FieldDefinition {
            name: "foo".to_string(),
            typ: dozer_types::types::FieldType::String,
            nullable: true,
        }],
        values: vec![0],
        primary_index: vec![0],
        secondary_indexes: vec![IndexDefinition {
            fields: vec![0],
            typ: IndexType::SortedInverted,
            sort_direction: vec![true],
        }],
    };
    schema
}
