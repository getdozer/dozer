use dozer_types::types::{
    FieldDefinition, IndexDefinition, Schema, SchemaIdentifier, SchemaWithIndex, SourceDefinition,
};

use super::expression::{FilterExpression, QueryExpression, Skip};

pub fn schema_0() -> SchemaWithIndex {
    (
        Schema {
            identifier: Some(SchemaIdentifier { id: 0, version: 1 }),
            fields: vec![FieldDefinition {
                name: "foo".to_string(),
                typ: dozer_types::types::FieldType::String,
                nullable: true,
                source: SourceDefinition::Dynamic,
            }],
            primary_index: vec![0],
        },
        vec![IndexDefinition::SortedInverted(vec![0])],
    )
}

pub fn schema_1() -> SchemaWithIndex {
    (
        Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "a".to_string(),
                    typ: dozer_types::types::FieldType::Int,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "b".to_string(),
                    typ: dozer_types::types::FieldType::String,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "c".to_string(),
                    typ: dozer_types::types::FieldType::Int,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![0],
        },
        vec![
            IndexDefinition::SortedInverted(vec![0]),
            IndexDefinition::SortedInverted(vec![1]),
            IndexDefinition::SortedInverted(vec![2]),
            // composite index
            IndexDefinition::SortedInverted(vec![0, 1]),
        ],
    )
}

pub fn schema_full_text() -> SchemaWithIndex {
    (
        Schema {
            identifier: Some(SchemaIdentifier { id: 2, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "foo".to_string(),
                    typ: dozer_types::types::FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "bar".to_string(),
                    typ: dozer_types::types::FieldType::Text,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![0],
        },
        vec![IndexDefinition::FullText(0), IndexDefinition::FullText(1)],
    )
}

// This is for testing appending only schema, which doesn't need a primary index, for example, eth logs.
pub fn schema_empty_primary_index() -> SchemaWithIndex {
    (
        Schema {
            identifier: Some(SchemaIdentifier { id: 3, version: 1 }),
            fields: vec![FieldDefinition {
                name: "foo".to_string(),
                typ: dozer_types::types::FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            }],
            primary_index: vec![],
        },
        vec![IndexDefinition::SortedInverted(vec![0])],
    )
}

pub fn schema_multi_indices() -> SchemaWithIndex {
    (
        Schema {
            identifier: Some(SchemaIdentifier { id: 4, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    typ: dozer_types::types::FieldType::Int,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "text".to_string(),
                    typ: dozer_types::types::FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![0],
        },
        vec![
            IndexDefinition::SortedInverted(vec![0]),
            IndexDefinition::FullText(1),
        ],
    )
}

pub fn query_from_filter(filter: FilterExpression) -> QueryExpression {
    QueryExpression::new(Some(filter), vec![], Some(10), Skip::Skip(0))
}
