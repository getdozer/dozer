use dozer_storage::lmdb::Cursor;
use dozer_types::types::{Field, Record, Schema};

use crate::cache::{LmdbRwCache, RwCache};

pub fn insert_rec_1(
    cache: &LmdbRwCache,
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
        None,
    );
    cache.insert(&record).unwrap();
}

pub fn insert_full_text(
    cache: &LmdbRwCache,
    schema: &Schema,
    (a, b): (Option<String>, Option<String>),
) {
    let record = Record::new(
        schema.identifier,
        vec![
            a.map_or(Field::Null, Field::String),
            b.map_or(Field::Null, Field::Text),
        ],
        None,
    );
    cache.insert(&record).unwrap();
}

pub fn get_indexes(cache: &LmdbRwCache) -> Vec<Vec<(&[u8], &[u8])>> {
    let (txn, secondary_indexes) = cache.get_txn_and_secondary_indexes();
    let txn = txn.read();

    let mut items = Vec::new();
    for db in secondary_indexes.read().values() {
        let mut cursor = db.open_ro_cursor(txn.txn()).unwrap();
        items.push(
            cursor
                .iter_dup()
                .flatten()
                .collect::<dozer_storage::lmdb::Result<Vec<_>>>()
                .unwrap(),
        );
    }
    items
}
