use dozer_types::types::{
    FieldDefinition, FieldIndexAndDirection, IndexDefinition, Schema, SchemaIdentifier,
    SortDirection::Ascending,
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
        secondary_indexes: vec![IndexDefinition::SortedInverted {
            fields: vec![FieldIndexAndDirection {
                index: 0,
                direction: Ascending,
            }],
        }],
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
            IndexDefinition::SortedInverted {
                fields: vec![FieldIndexAndDirection {
                    index: 0,
                    direction: Ascending,
                }],
            },
            IndexDefinition::SortedInverted {
                fields: vec![FieldIndexAndDirection {
                    index: 1,
                    direction: Ascending,
                }],
            },
            IndexDefinition::SortedInverted {
                fields: vec![FieldIndexAndDirection {
                    index: 2,
                    direction: Ascending,
                }],
            },
            // composite index
            IndexDefinition::SortedInverted {
                fields: vec![
                    FieldIndexAndDirection {
                        index: 0,
                        direction: Ascending,
                    },
                    FieldIndexAndDirection {
                        index: 1,
                        direction: Ascending,
                    },
                ],
            },
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
        secondary_indexes: vec![IndexDefinition::FullText { field_index: 0 }],
    }
}
