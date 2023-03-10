use dozer_types::types::{Field, IndexDefinition, Record, Schema, SchemaWithIndex};

use crate::cache::{
    lmdb::cache::{LmdbCache, LmdbRwCache, SecondaryEnvironment},
    RoCache, RwCache,
};

pub fn create_cache(
    schema_gen: impl FnOnce() -> SchemaWithIndex,
) -> (LmdbRwCache, Schema, Vec<IndexDefinition>) {
    let schema = schema_gen();
    let cache = LmdbRwCache::create(&schema, &Default::default(), Default::default()).unwrap();
    (cache, schema.0, schema.1)
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

pub fn get_index_counts<C: LmdbCache>(cache: &C) -> Vec<usize> {
    (0..cache.get_schema().1.len())
        .map(|index| cache.secondary_env(index).count_data().unwrap())
        .collect()
}
