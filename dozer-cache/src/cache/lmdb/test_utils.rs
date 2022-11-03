use dozer_types::types::{Field, Record, Schema};
use lmdb::{Cursor, RoTransaction, Transaction};

use crate::cache::{Cache, LmdbCache};

pub fn insert_rec_1(cache: &LmdbCache, schema: &Schema, (a, b, c): (i64, String, i64)) {
    let record = Record::new(
        schema.identifier.clone(),
        vec![Field::Int(a), Field::String(b), Field::Int(c)],
    );
    cache.insert(&record).unwrap();
}

pub fn get_indexes(cache: &LmdbCache) -> Vec<Vec<(&[u8], &[u8])>> {
    let (env, index_metadata) = cache.get_index_metadata();
    let txn: RoTransaction = env.begin_ro_txn().unwrap();

    let indexes = index_metadata.get_all_raw();
    let mut items = Vec::new();
    for (_, db) in indexes {
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
