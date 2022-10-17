use dozer_types::types::{FieldDefinition, IndexDefinition, IndexType, Schema, SchemaIdentifier};

use super::lmdb::cache::LmdbCache;

pub fn setup() -> (LmdbCache, Schema) {
    let schema = schema_0();
    let cache = LmdbCache::new(true);
    (cache, schema)
}

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
        secondary_indexes: vec![IndexDefinition {
            fields: vec![0],
            typ: IndexType::SortedInverted,
            sort_direction: vec![true],
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
            IndexDefinition {
                fields: vec![0],
                typ: IndexType::SortedInverted,
                sort_direction: vec![true],
            },
            IndexDefinition {
                fields: vec![1],
                typ: IndexType::SortedInverted,
                sort_direction: vec![true],
            },
            IndexDefinition {
                fields: vec![2],
                typ: IndexType::SortedInverted,
                sort_direction: vec![true],
            },
            // composite index
            IndexDefinition {
                fields: vec![0, 1],
                typ: IndexType::SortedInverted,
                sort_direction: vec![true, true],
            },
        ],
    }
}
