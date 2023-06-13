use std::pin::pin;

use lmdb::Cursor;
use lmdb_sys::{MDB_FIRST, MDB_NEXT, MDB_NEXT_DUP};

use crate::{
    generator::{FutureGeneratorContext, Generator, IntoGenerator},
    yield_return_if_err,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DupData<'txn> {
    First { key: &'txn [u8], value: &'txn [u8] },
    Following { value: &'txn [u8] },
}

/// A `Generator` that yields all `DupData` in given `cursor`.
///
/// The cursor must be opened with a database that has `DUP_SORT` enabled.
pub async fn dup_data<'txn, C: Cursor<'txn>>(
    cursor: C,
    context: FutureGeneratorContext<Result<DupData<'txn>, lmdb::Error>>,
) -> C {
    match get_one_dup_data(&cursor, MDB_FIRST, &context, true).await {
        Ok(Some(())) => (),
        _ => return cursor,
    }
    if let Err(()) = get_following_dup_data(&cursor, &context).await {
        return cursor;
    }

    loop {
        match get_one_dup_data(&cursor, MDB_NEXT, &context, true).await {
            Ok(Some(())) => (),
            _ => return cursor,
        }
        if let Err(()) = get_following_dup_data(&cursor, &context).await {
            return cursor;
        }
    }
}

/// A `Generator` that yields number of value items in each key.
///
/// The cursor must be opened with a database that has `DUP_SORT` enabled.
pub async fn dup_value_counts<'txn, C: Cursor<'txn>>(
    cursor: C,
    context: FutureGeneratorContext<Result<u64, lmdb::Error>>,
) -> Result<(), ()> {
    let generator = pin!((|context| dup_data(cursor, context)).into_generator());
    let mut iter = generator.into_iter();
    let mut current_count = None;
    loop {
        match iter.next() {
            Some(result) => match yield_return_if_err!(context, result) {
                DupData::First { .. } => {
                    if let Some(count) = current_count {
                        context.yield_(Ok(count)).await;
                    }
                    current_count = Some(1);
                }
                DupData::Following { .. } => {
                    current_count = Some(current_count.expect("Must be `Some`") + 1);
                }
            },
            None => {
                if let Some(count) = current_count {
                    context.yield_(Ok(count)).await;
                }
                return Ok(());
            }
        }
    }
}

/// Gets one `DupData` using given `op`.
///
/// # Arguments
///
/// - `cursor`: The cursor to use.
/// - `op`: The operation to use, `MDB_FIRST`, `MDB_NEXT`, `MDB_NEXT_DUP`, etc.
/// - `context`: The context to yield the result.
/// - `first`: Whether the yielded `DupData` is the first one. If `true`, will yield `DupData::First`, otherwise `DupData::Following`.
///
/// # Return
///
/// - `Ok(Some(()))`: Successfully yielded one `DupData`.
/// - `Ok(None)`: Using given `op`, the cursor get result is `MDB_NOTFOUND`.
/// - `Err(())`: Yielded an error.
///
/// `cursor` must not be used by other functions between the await points.
async fn get_one_dup_data<'txn, C: Cursor<'txn>>(
    cursor: &C,
    op: u32,
    context: &FutureGeneratorContext<Result<DupData<'txn>, lmdb::Error>>,
    first: bool,
) -> Result<Option<()>, ()> {
    match cursor.get(None, None, op) {
        Ok((key, value)) => {
            let key = key.expect("get_first_dup_data should always return a key");
            if first {
                context.yield_(Ok(DupData::First { key, value })).await;
            } else {
                context.yield_(Ok(DupData::Following { value })).await;
            }
            Ok(Some(()))
        }
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => {
            context.yield_(Err(e)).await;
            Err(())
        }
    }
}

/// Gets all following `DupData` using `MDB_NEXT_DUP` until `MDB_NOTFOUND`.
/// Yields `Err(())` and returns if any error occurs.
///
/// # Return
///
/// - `Ok(())`: Successfully yielded all following `DupData`.
/// - `Err(())`: Yielded an error.
///
/// `cursor` must not be used by other functions between the await points.
async fn get_following_dup_data<'txn, C: Cursor<'txn>>(
    cursor: &C,
    context: &FutureGeneratorContext<Result<DupData<'txn>, lmdb::Error>>,
) -> Result<(), ()> {
    loop {
        match get_one_dup_data(cursor, MDB_NEXT_DUP, context, false).await {
            Ok(Some(())) => (),
            Ok(None) => return Ok(()),
            Err(()) => return Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use lmdb::{Database, DatabaseFlags, Transaction};
    use tempdir::TempDir;

    use crate::{
        generator::{Generator, IntoGenerator},
        lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
        LmdbEnvironment, RwLmdbEnvironment,
    };

    use super::*;

    fn setup_test_env() -> (TempDir, RwLmdbEnvironment, Database) {
        let temp_dir = TempDir::new("test").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "dump_tests_env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let db = env
            .create_database(Some("test_dup_data_generator"), DatabaseFlags::DUP_SORT)
            .unwrap();

        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"key1", b"value1"),
            (b"key1", b"value2"),
            (b"key2", b"value3"),
            (b"key2", b"value4"),
            (b"key2", b"value5"),
        ];
        let txn = env.txn_mut().unwrap();
        for (key, value) in data.iter() {
            txn.put(db, key, value, lmdb::WriteFlags::empty()).unwrap();
        }
        env.commit().unwrap();

        (temp_dir, env, db)
    }

    #[test]
    fn test_dup_data_generator() {
        let (_temp_dir, env, db) = setup_test_env();

        let txn = env.begin_txn().unwrap();
        let cursor = txn.open_ro_cursor(db).unwrap();
        let generator = pin!((|context| dup_data(cursor, context)).into_generator());
        let mut iter = generator.into_iter();

        let DupData::First { key, value } = iter.next().unwrap().unwrap() else {
            panic!("Must be first");
        };
        assert_eq!(key, b"key1");
        assert_eq!(value, b"value1");

        let DupData::Following { value } = iter.next().unwrap().unwrap() else {
            panic!("Must be following");
        };
        assert_eq!(value, b"value2");

        let DupData::First { key, value } = iter.next().unwrap().unwrap() else {
            panic!("Must be first");
        };
        assert_eq!(key, b"key2");
        assert_eq!(value, b"value3");

        let DupData::Following { value } = iter.next().unwrap().unwrap() else {
            panic!("Must be following");
        };
        assert_eq!(value, b"value4");

        let DupData::Following { value } = iter.next().unwrap().unwrap() else {
            panic!("Must be following");
        };
        assert_eq!(value, b"value5");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dup_value_counts() {
        let (_temp_dir, env, db) = setup_test_env();

        let txn = env.begin_txn().unwrap();
        let cursor = txn.open_ro_cursor(db).unwrap();
        let generator = pin!((|context| dup_value_counts(cursor, context)).into_generator());
        assert_eq!(
            generator
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![2, 3]
        );
    }
}
