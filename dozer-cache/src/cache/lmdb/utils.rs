use std::{fs, ops::Deref};

use crate::errors::CacheError;
use dozer_storage::{
    lmdb::EnvironmentFlags,
    lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
};
use tempdir::TempDir;

use super::cache::{CacheCommonOptions, CacheWriteOptions};

#[derive(Clone, Debug, Default)]
pub struct CacheOptions {
    pub common: CacheCommonOptions,
    pub kind: CacheOptionsKind,
}

#[derive(Clone, Debug, Default)]
pub struct CacheReadOptions {}

#[derive(Clone, Debug)]
pub enum CacheOptionsKind {
    // Write Options
    Write(CacheWriteOptions),

    // Read Options
    ReadOnly(CacheReadOptions),
}

impl Default for CacheOptionsKind {
    fn default() -> Self {
        Self::Write(CacheWriteOptions::default())
    }
}

pub fn init_env(options: &CacheOptions) -> Result<(LmdbEnvironmentManager, String), CacheError> {
    match &options.kind {
        CacheOptionsKind::Write(write_options) => {
            let (base_path, name, _temp_dir) = match &options.common.path {
                None => {
                    let base_path =
                        TempDir::new("dozer").map_err(|e| CacheError::Internal(Box::new(e)))?;
                    (
                        base_path.path().to_path_buf(),
                        "dozer-cache",
                        Some(base_path),
                    )
                }
                Some((base_path, name)) => {
                    fs::create_dir_all(base_path).map_err(|e| CacheError::Internal(Box::new(e)))?;
                    (base_path.clone(), name.deref(), None)
                }
            };

            let options = LmdbEnvironmentOptions::new(
                options.common.max_db_size,
                options.common.max_readers,
                write_options.max_size,
                EnvironmentFlags::empty(),
            );

            Ok((
                LmdbEnvironmentManager::create(&base_path, name, options)?,
                name.to_string(),
            ))
        }
        CacheOptionsKind::ReadOnly(_) => {
            let (base_path, name) = options
                .common
                .path
                .as_ref()
                .ok_or(CacheError::PathNotInitialized)?;

            let env_options = LmdbEnvironmentOptions {
                max_dbs: options.common.max_db_size,
                max_readers: options.common.max_readers,
                flags: EnvironmentFlags::READ_ONLY,
                ..Default::default()
            };

            Ok((
                LmdbEnvironmentManager::create(base_path, name, env_options)?,
                name.to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb::{Cursor, DatabaseFlags, RoCursor, Transaction, WriteFlags};
    use dozer_types::types::Field;

    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    fn cursor_dump(mut cursor: RoCursor) -> Vec<(&[u8], &[u8])> {
        cursor
            .iter_dup()
            .flatten()
            .collect::<dozer_storage::lmdb::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn duplicate_test_nested() {
        let options = CacheOptions::default();
        let mut env = init_env(&options).unwrap().0;

        let db = env
            .create_database(
                Some("test"),
                Some(DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_KEY),
            )
            .unwrap();

        let txn = env.create_txn().unwrap();
        let mut master_txn = txn.write();
        let txn = master_txn.txn_mut();

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
        for (key, data) in &items {
            let key = [
                "idx".as_bytes().to_vec(),
                Field::Int(*key).encode(),
                key.to_be_bytes().to_vec(),
            ]
            .join("#".as_bytes());
            c_txn.put(db, &key, data, WriteFlags::empty()).unwrap();
        }
        c_txn.commit().unwrap();
        master_txn.commit_and_renew().unwrap();

        let rtxn = master_txn.txn();

        let cursor = rtxn.open_ro_cursor(db).unwrap();
        let vals = cursor_dump(cursor);
        assert_eq!(vals.len(), items.len(), "must have duplicate records");
    }
}
