use std::sync::Arc;

use super::registry::{context, SchemaRegistryClient};
use dozer_types::types::{
    FieldDefinition, IndexDefinition, IndexType, Schema, SchemaIdentifier,
};

pub async fn init_schema(client: Arc<SchemaRegistryClient>) -> Schema {
    // Initialize Schema Registry
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

    // Insert Schema
    client
        .insert(context::current(), schema.clone())
        .await
        .unwrap();

    schema
}
