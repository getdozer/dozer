use dozer_types::types::{
    FieldDefinition, IndexDefinition, Schema, SchemaIdentifier, SortDirection::Ascending,
};

pub fn schema_0() -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![FieldDefinition {
            name: "foo".to_string(),
            typ: dozer_types::types::FieldType::String,
            nullable: true,
        }],
        values: vec![0],
        primary_index: vec![0],
        secondary_indexes: vec![IndexDefinition::SortedInverted(vec![(0, Ascending)])],
    }
}

pub fn schema_1() -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![
            FieldDefinition {
                name: "a".to_string(),
                typ: dozer_types::types::FieldType::Int,
                nullable: true,
            },
            FieldDefinition {
                name: "b".to_string(),
                typ: dozer_types::types::FieldType::String,
                nullable: true,
            },
            FieldDefinition {
                name: "c".to_string(),
                typ: dozer_types::types::FieldType::Int,
                nullable: true,
            },
        ],
        values: vec![0],
        primary_index: vec![0],
        secondary_indexes: vec![
            IndexDefinition::SortedInverted(vec![(0, Ascending)]),
            IndexDefinition::SortedInverted(vec![(1, Ascending)]),
            IndexDefinition::SortedInverted(vec![(2, Ascending)]),
            // composite index
            IndexDefinition::SortedInverted(vec![(0, Ascending), (1, Ascending)]),
        ],
    }
}

pub fn schema_full_text_single() -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![FieldDefinition {
            name: "foo".to_string(),
            typ: dozer_types::types::FieldType::String,
            nullable: false,
        }],
        values: vec![0],
        primary_index: vec![0],
        secondary_indexes: vec![IndexDefinition::SortedInverted(vec![(0, Ascending)])],
    }
}
