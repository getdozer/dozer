use dozer_types::types::{
    Field, FieldDefinition, IndexDefinition, IndexType, Record, Schema, SchemaIdentifier,
};
use lmdb::{Cursor, RoTransaction, Transaction};

use super::{Cache, LmdbCache};

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
            // composite index [a, b]
            IndexDefinition {
                fields: vec![0, 1],
                typ: IndexType::SortedInverted,
                sort_direction: vec![true, true],
            },
        ],
    }
}

pub fn insert_rec_1(cache: &LmdbCache, schema: &Schema, (a, b, c): (i64, String, i64)) {
    let record = Record::new(
        schema.identifier.clone(),
        vec![Field::Int(a), Field::String(b), Field::Int(c)],
    );
    cache.insert(&record).unwrap();
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
        secondary_indexes: vec![IndexDefinition {
            fields: vec![0],
            typ: IndexType::FullText,
            sort_direction: vec![true],
        }],
    }
}

pub fn get_indexes(cache: &LmdbCache) -> Vec<(&[u8], &[u8])> {
    let (env, indexer_db) = cache.get_index_db();
    let txn: RoTransaction = env.begin_ro_txn().unwrap();
    let mut cursor = txn.open_ro_cursor(*indexer_db).unwrap();
    cursor
        .iter_dup()
        .flatten()
        .collect::<lmdb::Result<Vec<_>>>()
        .unwrap()
}
