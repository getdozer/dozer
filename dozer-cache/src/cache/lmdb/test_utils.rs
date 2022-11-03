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
