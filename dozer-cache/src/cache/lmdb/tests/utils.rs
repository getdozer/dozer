use dozer_types::types::{Field, Record, Schema};
use lmdb::{Cursor, RoTransaction, Transaction};

use crate::cache::{Cache, LmdbCache};

pub fn insert_rec_1(
    cache: &LmdbCache,
    schema: &Schema,
    (a, b, c): (i64, Option<String>, Option<i64>),
) {
    let record = Record::new(
        schema.identifier.clone(),
        vec![
            Field::Int(a),
            b.map_or(Field::Null, Field::String),
            c.map_or(Field::Null, Field::Int),
        ],
    );
    cache.insert(&record).unwrap();
}

pub fn get_indexes(cache: &LmdbCache) -> Vec<Vec<(&[u8], &[u8])>> {
    let (env, secondary_indexes) = cache.get_env_and_secondary_indexes();
    let txn: RoTransaction = env.begin_ro_txn().unwrap();

    let mut items = Vec::new();
    for db in secondary_indexes.read().values().copied() {
        let mut cursor = txn.open_ro_cursor(db).unwrap();
        items.push(
            cursor
                .iter_dup()
                .flatten()
                .collect::<lmdb::Result<Vec<_>>>()
                .unwrap(),
        );
    }
    items
}
