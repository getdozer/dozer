use std::{
    fs,
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::errors::CacheError;
use dozer_storage::{
    lmdb::EnvironmentFlags,
    lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
};
use tempdir::TempDir;

use super::cache::{CacheCommonOptions, CacheWriteOptions};

#[allow(clippy::type_complexity)]
pub fn init_env(
    common_options: &CacheCommonOptions,
    write_options: Option<CacheWriteOptions>,
) -> Result<(LmdbEnvironmentManager, (PathBuf, String), Option<TempDir>), CacheError> {
    if let Some(write_options) = write_options {
        create_env(common_options, write_options)
    } else {
        let (env, (base_path, name), temp_dir) = open_env(common_options)?;
        Ok((env, (base_path.to_path_buf(), name.to_string()), temp_dir))
    }
}

#[allow(clippy::type_complexity)]
fn create_env(
    common_options: &CacheCommonOptions,
    write_options: CacheWriteOptions,
) -> Result<(LmdbEnvironmentManager, (PathBuf, String), Option<TempDir>), CacheError> {
    let (base_path, name, temp_dir) = match &common_options.path {
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
        common_options.max_db_size,
        common_options.max_readers,
        write_options.max_size,
        EnvironmentFlags::empty(),
    );

    Ok((
        LmdbEnvironmentManager::create(&base_path, name, options)?,
        (base_path, name.to_string()),
        temp_dir,
    ))
}

#[allow(clippy::type_complexity)]
fn open_env(
    options: &CacheCommonOptions,
) -> Result<(LmdbEnvironmentManager, (&Path, &str), Option<TempDir>), CacheError> {
    let (base_path, name) = options
        .path
        .as_ref()
        .ok_or(CacheError::PathNotInitialized)?;

    let env_options = LmdbEnvironmentOptions {
        max_dbs: options.max_db_size,
        max_readers: options.max_readers,
        flags: EnvironmentFlags::READ_ONLY,
        ..Default::default()
    };

    Ok((
        LmdbEnvironmentManager::create(base_path, name, env_options)?,
        (base_path, name),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use dozer_storage::{
        lmdb::{Cursor, DatabaseFlags, RoCursor, Transaction, WriteFlags},
        lmdb_storage::CreateDatabase,
    };
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
        let mut env = create_env(&Default::default(), Default::default())
            .unwrap()
            .0;

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
