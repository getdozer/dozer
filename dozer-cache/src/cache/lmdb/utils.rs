use std::fs;
use std::path::Path;

use crate::errors::CacheError;
use lmdb::{Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor};
use tempdir::TempDir;

use super::{CacheOptions, CacheOptionsKind};

pub fn init_env(options: &CacheOptions) -> Result<Environment, CacheError> {
    match &options.kind {
        CacheOptionsKind::Write(write_options) => {
            let mut env = Environment::new();

            let env = env
                .set_max_readers(options.common.max_readers)
                .set_map_size(write_options.max_size)
                .set_max_dbs(options.common.max_db_size);

            let env = match &options.common.path {
                None => env
                    .open(
                        TempDir::new("dozer")
                            .map_err(|e| CacheError::InternalError(Box::new(e)))?
                            .path()
                            .as_ref(),
                    )
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?,
                Some(path) => {
                    fs::create_dir_all(path).map_err(|e| CacheError::InternalError(Box::new(e)))?;
                    env.open(Path::new(&path))
                        .map_err(|e| CacheError::InternalError(Box::new(e)))?
                }
            };
            Ok(env)
        }
        CacheOptionsKind::ReadOnly(_) => {
            let mut env = Environment::new();
            let env = env
                .set_flags(EnvironmentFlags::READ_ONLY)
                .set_max_dbs(options.common.max_db_size);

            env.open(Path::new(
                &options
                    .common
                    .path
                    .as_ref()
                    .map_or(Err(CacheError::PathNotInitialized), Ok)?,
            ))
            .map_err(|e| CacheError::InternalError(Box::new(e)))
        }
    }
}

pub fn init_db(
    env: &Environment,
    name: Option<&str>,
    options: &CacheOptions,
    allow_dup: bool,
    fixed_length_key: bool,
) -> Result<Database, CacheError> {
    match options.kind {
        CacheOptionsKind::Write(_) => {
            let mut flags = DatabaseFlags::default();
            if allow_dup {
                flags.set(DatabaseFlags::DUP_SORT, true);
                if fixed_length_key {
                    flags.set(DatabaseFlags::INTEGER_DUP, true);
                }
            } else if fixed_length_key {
                flags.set(DatabaseFlags::INTEGER_KEY, true);
            }

            let db = env
                .create_db(name, flags)
                .map_err(|e| CacheError::InternalError(Box::new(e)))?;

            Ok(db)
        }
        CacheOptionsKind::ReadOnly(_) => {
            let db = env
                .open_db(name)
                .map_err(|e| CacheError::InternalError(Box::new(e)))?;
            Ok(db)
        }
    }
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

    use crate::cache::lmdb::{
        utils::{_cursor_dump, init_db, init_env},
        CacheOptions,
    };

    #[test]
    fn duplicate_test_nested() {
        let options = CacheOptions::default();
        let env = init_env(&options).unwrap();

        let db = init_db(&env, Some("test"), &options, true, true).unwrap();

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
                Field::Int(*key).to_bytes(),
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
