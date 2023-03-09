use dozer_storage::{lmdb::Transaction, LmdbMultimap};
use dozer_types::types::{Field, IndexDefinition, Record, Schema};

use crate::cache::{lmdb::cache::LmdbRwCache, RwCache};

pub fn create_cache(
    schema_gen: impl FnOnce() -> (Schema, Vec<IndexDefinition>),
) -> (LmdbRwCache, Schema, Vec<IndexDefinition>) {
    let (schema, secondary_indexes) = schema_gen();
    let cache = LmdbRwCache::create(
        schema.clone(),
        secondary_indexes.clone(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    (cache, schema, secondary_indexes)
}

pub fn insert_rec_1(
    cache: &LmdbRwCache,
    schema: &Schema,
    (a, b, c): (i64, Option<String>, Option<i64>),
) {
    let mut record = Record::new(
        schema.identifier,
        vec![
            Field::Int(a),
            b.map_or(Field::Null, Field::String),
            c.map_or(Field::Null, Field::Int),
        ],
        None,
    );
    cache.insert(&mut record).unwrap();
}

pub fn insert_full_text(
    cache: &LmdbRwCache,
    schema: &Schema,
    (a, b): (Option<String>, Option<String>),
) {
    let mut record = Record::new(
        schema.identifier,
        vec![
            a.map_or(Field::Null, Field::String),
            b.map_or(Field::Null, Field::Text),
        ],
        None,
    );
    cache.insert(&mut record).unwrap();
}

pub fn get_index_counts<T: Transaction>(
    txn: &T,
    secondary_index_databases: &[LmdbMultimap<Vec<u8>, u64>],
) -> Vec<usize> {
    let mut items = Vec::new();
    for db in secondary_index_databases {
        items.push(db.count_data(txn).unwrap());
    }
    items
}
