use std::sync::Arc;

use dozer_types::parking_lot::Mutex;
use dozer_types::types::{Field, IndexDefinition, Record, Schema, SchemaWithIndex};

use crate::cache::{
    lmdb::{
        cache::{LmdbCache, LmdbRwCache, SecondaryEnvironment},
        indexing::IndexingThreadPool,
    },
    RoCache, RwCache,
};

pub fn create_cache(
    schema_gen: impl FnOnce() -> SchemaWithIndex,
) -> (
    LmdbRwCache,
    Arc<Mutex<IndexingThreadPool>>,
    Schema,
    Vec<IndexDefinition>,
) {
    let schema = schema_gen();
    let indexing_thread_pool = Arc::new(Mutex::new(IndexingThreadPool::new(1)));
    let cache = LmdbRwCache::new(
        Some(&schema),
        &Default::default(),
        Default::default(),
        indexing_thread_pool.clone(),
    )
    .unwrap();
    (cache, indexing_thread_pool, schema.0, schema.1)
}

pub fn insert_rec_1(
    cache: &mut LmdbRwCache,
    schema: &Schema,
    (a, b, c): (i64, Option<String>, Option<i64>),
) {
    let record = Record::new(
        schema.identifier,
        vec![
            Field::Int(a),
            b.map_or(Field::Null, Field::String),
            c.map_or(Field::Null, Field::Int),
        ],
    );
    cache.insert(&record).unwrap();
}

pub fn insert_full_text(
    cache: &mut LmdbRwCache,
    schema: &Schema,
    (a, b): (Option<String>, Option<String>),
) {
    let record = Record::new(
        schema.identifier,
        vec![
            a.map_or(Field::Null, Field::String),
            b.map_or(Field::Null, Field::Text),
        ],
    );
    cache.insert(&record).unwrap();
}

pub fn get_index_counts<C: LmdbCache>(cache: &C) -> Vec<usize> {
    (0..cache.get_schema().1.len())
        .map(|index| cache.secondary_env(index).count_data().unwrap())
        .collect()
}
