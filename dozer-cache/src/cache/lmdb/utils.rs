use std::fs;
use std::path::Path;

use dozer_types::errors::cache::CacheError;
use lmdb::{Cursor, Database, DatabaseFlags, Environment, RoCursor};
use tempdir::TempDir;

pub fn init_env(temp: bool) -> Result<Environment, CacheError> {
    let map_size = 1024 * 1024 * 1024 * 5;
    let mut env = Environment::new();

    let env = env
        .set_max_readers(10)
        .set_map_size(map_size)
        .set_max_dbs(10)
        .set_map_size(map_size);

    let env = match temp {
        true => env
            .open(
                TempDir::new("cache")
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?
                    .path(),
            )
            .map_err(|e| CacheError::InternalError(Box::new(e)))?,
        false => {
            fs::create_dir_all("target/cache.mdb")
                .map_err(|e| CacheError::InternalError(Box::new(e)))?;
            env.open(Path::new("target/cache.mdb"))
                .map_err(|e| CacheError::InternalError(Box::new(e)))?
        }
    };

    Ok(env)
}

pub fn init_db(env: &Environment, name: Option<&str>) -> Result<Database, CacheError> {
    let db = env
        .create_db(name, DatabaseFlags::DUP_SORT)
        .map_err(|e| CacheError::InternalError(Box::new(e)))?;

    Ok(db)
}

pub fn _cursor_dump(mut cursor: RoCursor) -> Vec<(&[u8], &[u8])> {
    cursor
        .iter_dup()
        .flatten()
        .collect::<lmdb::Result<Vec<_>>>()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use dozer_types::types::Field;
    use lmdb::{Transaction, WriteFlags};

    use crate::cache::lmdb::utils::{_cursor_dump, init_db, init_env};

    #[test]
    fn duplicate_test_nested() {
        let env = init_env(true).unwrap();

        let db = init_db(&env, Some("test")).unwrap();

        let mut txn = env.begin_rw_txn().unwrap();

        let mut c_txn = txn.begin_nested_txn().unwrap();

        let items: Vec<(i64, &[u8])> = vec![
            (1, b"a"),
            (2, b"a"),
            (3, b"a"),
            (1, b"b"),
            (2, b"b"),
            (3, b"b"),
            (1, b"c"),
            (2, b"c"),
            (3, b"c"),
            (1, b"e"),
            (2, b"e"),
            (3, b"e"),
        ];
        for &(ref key, ref data) in &items {
            let key = [
                "idx".as_bytes().to_vec(),
                Field::Int(*key).to_bytes().unwrap(),
                key.to_be_bytes().to_vec(),
            ]
            .join("#".as_bytes());
            c_txn.put(db, &key, data, WriteFlags::empty()).unwrap();
        }
        c_txn.commit().unwrap();
        txn.commit().unwrap();

        let rtxn = env.begin_ro_txn().unwrap();

        let cursor = rtxn.open_ro_cursor(db).unwrap();
        let vals = _cursor_dump(cursor);
        assert_eq!(vals.len(), items.len(), "must have duplicate records");
    }
}
