use std::{
    fs,
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::errors::CacheError;
use dozer_storage::{
    lmdb::EnvironmentFlags,
    lmdb_storage::{
        LmdbEnvironmentManager, LmdbEnvironmentOptions, RoLmdbEnvironment, RwLmdbEnvironment,
    },
};
use tempdir::TempDir;

use super::cache::CacheOptions;

#[allow(clippy::type_complexity)]
pub fn create_env(
    options: &CacheOptions,
) -> Result<(RwLmdbEnvironment, (PathBuf, String), Option<TempDir>), CacheError> {
    let (base_path, name, temp_dir) = match &options.path {
        None => {
            let base_path =
                TempDir::new("dozer").map_err(|e| CacheError::Io("tempdir".into(), e))?;
            (
                base_path.path().to_path_buf(),
                "dozer-cache",
                Some(base_path),
            )
        }
        Some((base_path, name)) => {
            fs::create_dir_all(base_path).map_err(|e| CacheError::Io(base_path.clone(), e))?;
            (base_path.clone(), name.deref(), None)
        }
    };

    let options = LmdbEnvironmentOptions::new(
        options.max_db_size,
        options.max_readers,
        options.max_size,
        EnvironmentFlags::empty(),
    );

    Ok((
        LmdbEnvironmentManager::create_rw(&base_path, name, options)?,
        (base_path, name.to_string()),
        temp_dir,
    ))
}

#[allow(clippy::type_complexity)]
pub fn open_env(
    options: &CacheOptions,
) -> Result<(RoLmdbEnvironment, (&Path, &str), Option<TempDir>), CacheError> {
    let (base_path, name) = options
        .path
        .as_ref()
        .ok_or(CacheError::PathNotInitialized)?;

    let env_options = LmdbEnvironmentOptions::new(
        options.max_db_size,
        options.max_readers,
        options.max_size,
        EnvironmentFlags::empty(),
    );

    Ok((
        LmdbEnvironmentManager::create_ro(base_path, name, env_options)?,
        (base_path, name),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb::{Cursor, DatabaseFlags, RoCursor, Transaction, WriteFlags};
    use dozer_types::types::Field;

    use super::*;

    fn cursor_dump(mut cursor: RoCursor) -> Vec<(&[u8], &[u8])> {
        cursor
            .iter_dup()
            .flatten()
            .collect::<dozer_storage::lmdb::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn duplicate_test_nested() {
        let mut env = create_env(&Default::default()).unwrap().0;

        let db = env
            .create_database(
                Some("test"),
                DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_KEY,
            )
            .unwrap();

        let txn = env.txn_mut().unwrap();

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
        env.commit().unwrap();

        let rtxn = env.txn_mut().unwrap();

        let cursor = rtxn.open_ro_cursor(db).unwrap();
        let vals = cursor_dump(cursor);
        assert_eq!(vals.len(), items.len(), "must have duplicate records");
    }
}
