use std::ops::Bound;

use lmdb::Cursor;
use lmdb_sys::{
    MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_NEXT_NODUP, MDB_PREV, MDB_PREV_NODUP, MDB_SET_RANGE,
};

use crate::errors::StorageError;

type KeyValuePair<'txn> = (&'txn [u8], &'txn [u8]);

enum IteratorState<'txn> {
    First {
        item: Option<KeyValuePair<'txn>>,
        ascending: bool,
    },
    NotFirst {
        ascending: bool,
    },
}

pub struct RawIterator<'txn, C: Cursor<'txn>> {
    cursor: C,
    state: IteratorState<'txn>,
}

impl<'txn, C: Cursor<'txn>> Iterator for RawIterator<'txn, C> {
    type Item = Result<(&'txn [u8], &'txn [u8]), lmdb::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.state {
            IteratorState::First { item, ascending } => {
                let item = *item;
                self.state = IteratorState::NotFirst {
                    ascending: *ascending,
                };
                item.map(Ok)
            }
            IteratorState::NotFirst { ascending } => if *ascending {
                cursor_get(&self.cursor, MDB_NEXT)
            } else {
                cursor_get(&self.cursor, MDB_PREV)
            }
            .transpose(),
        }
    }
}

impl<'txn, C: Cursor<'txn>> RawIterator<'txn, C> {
    pub fn new(
        cursor: C,
        starting_key: Bound<&[u8]>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let item = match (starting_key, ascending) {
            (Bound::Included(starting_key), true) => {
                cursor_get_greater_than_or_equal_to(&cursor, starting_key)
            }
            (Bound::Excluded(starting_key), true) => cursor_get_greater_than(&cursor, starting_key),
            (Bound::Unbounded, true) => cursor_get(&cursor, MDB_FIRST),
            (Bound::Included(starting_key), false) => {
                cursor_get_less_than_or_equal_to(&cursor, starting_key)
            }
            (Bound::Excluded(starting_key), false) => cursor_get_less_than(&cursor, starting_key),
            (Bound::Unbounded, false) => cursor_get(&cursor, MDB_LAST),
        }?;
        Ok(RawIterator {
            cursor,
            state: IteratorState::First { item, ascending },
        })
    }
}

fn cursor_get<'txn, C: Cursor<'txn>>(
    cursor: &C,
    op: u32,
) -> Result<Option<KeyValuePair<'txn>>, lmdb::Error> {
    match cursor.get(None, None, op) {
        Ok((key, value)) => Ok(Some((
            key.expect("Key should always be `Some` unless `op` is `MDB_SET`"),
            value,
        ))),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

fn cursor_get_greater_than_or_equal_to<'txn, 'key, C: Cursor<'txn>>(
    cursor: &C,
    key: &'key [u8],
) -> Result<Option<KeyValuePair<'txn>>, lmdb::Error> {
    match cursor.get(Some(key), None, MDB_SET_RANGE) {
        Ok((key, value)) => Ok(Some((
            key.expect("Key should always be `Some` unless `op` is `MDB_SET`"),
            value,
        ))),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

fn cursor_get_greater_than<'txn, 'key, C: Cursor<'txn>>(
    cursor: &C,
    key: &'key [u8],
) -> Result<Option<KeyValuePair<'txn>>, lmdb::Error> {
    match cursor_get_greater_than_or_equal_to(cursor, key)? {
        Some((hit_key, value)) => {
            if hit_key == key {
                // Hit equal key, get next.
                cursor_get(cursor, MDB_NEXT_NODUP)
            } else {
                // Hit greater key, return it.
                Ok(Some((hit_key, value)))
            }
        }
        None => Ok(None),
    }
}

fn cursor_get_less_than_or_equal_to<'txn, 'key, C: Cursor<'txn>>(
    cursor: &C,
    key: &'key [u8],
) -> Result<Option<KeyValuePair<'txn>>, lmdb::Error> {
    match cursor_get_greater_than_or_equal_to(cursor, key)? {
        Some((hit_key, value)) => {
            if hit_key == key {
                // Hit equal key, return it.
                Ok(Some((hit_key, value)))
            } else {
                // Hit greater key, get previous.
                cursor_get(cursor, MDB_PREV_NODUP)
            }
        }
        // All key less than given key, get last.
        None => cursor_get(cursor, MDB_LAST),
    }
}

fn cursor_get_less_than<'txn, 'key, C: Cursor<'txn>>(
    cursor: &C,
    key: &'key [u8],
) -> Result<Option<KeyValuePair<'txn>>, lmdb::Error> {
    match cursor_get_greater_than_or_equal_to(cursor, key)? {
        Some(_) => {
            // Hit greater or equal key, get previous.
            cursor_get(cursor, MDB_PREV_NODUP)
        }
        // All key less than given key, get last.
        None => cursor_get(cursor, MDB_LAST),
    }
}

#[cfg(test)]
mod tests {
    use lmdb::{Database, DatabaseFlags, Transaction, WriteFlags};
    use tempdir::TempDir;

    use crate::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions, RwLmdbEnvironment};

    use super::*;

    fn test_database() -> (TempDir, RwLmdbEnvironment, Database) {
        let temp_dir = TempDir::new("test_database").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "test_database",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let db = env.create_database(None, DatabaseFlags::DUP_SORT).unwrap();

        (temp_dir, env, db)
    }

    fn insert_test_data(txn: &mut RwLmdbEnvironment, db: Database) {
        for key in [b"1", b"3", b"5"] {
            txn.txn_mut()
                .unwrap()
                .put(db, key, &[], WriteFlags::empty())
                .unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn test_cursor_get_greater_than_or_equal_to() {
        let (_temp_dir, mut env, db) = test_database();

        // Empty database.
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            assert_eq!(
                cursor_get_greater_than_or_equal_to(&cursor, b"0").unwrap(),
                None
            );
        }

        // Non-empty database.
        insert_test_data(&mut env, db);
        let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
        // Before db start.
        assert_eq!(
            cursor_get_greater_than_or_equal_to(&cursor, b"0")
                .unwrap()
                .unwrap()
                .0,
            b"1"
        );
        // Equal.
        assert_eq!(
            cursor_get_greater_than_or_equal_to(&cursor, b"3")
                .unwrap()
                .unwrap()
                .0,
            b"3"
        );
        // Greater than.
        assert_eq!(
            cursor_get_greater_than_or_equal_to(&cursor, b"4")
                .unwrap()
                .unwrap()
                .0,
            b"5"
        );
        // Past db end.
        assert_eq!(
            cursor_get_greater_than_or_equal_to(&cursor, b"6").unwrap(),
            None
        );
    }

    #[test]
    fn test_cursor_greater_than() {
        let (_temp_dir, mut env, db) = test_database();

        // Empty database.
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            assert_eq!(cursor_get_greater_than(&cursor, b"0").unwrap(), None);
        }

        // Non-empty database.
        insert_test_data(&mut env, db);
        let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
        // Before db start.
        assert_eq!(
            cursor_get_greater_than(&cursor, b"0").unwrap().unwrap().0,
            b"1"
        );
        // Equal element should be skipped.
        assert_eq!(
            cursor_get_greater_than(&cursor, b"3").unwrap().unwrap().0,
            b"5"
        );
        // Greater than.
        assert_eq!(
            cursor_get_greater_than(&cursor, b"4").unwrap().unwrap().0,
            b"5"
        );
        // Past db end.
        assert_eq!(cursor_get_greater_than(&cursor, b"6").unwrap(), None);
    }

    #[test]
    fn test_cursor_get_less_than_or_equal_to() {
        let (_temp_dir, mut env, db) = test_database();

        // Empty database.
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            assert_eq!(
                cursor_get_less_than_or_equal_to(&cursor, b"6").unwrap(),
                None
            );
        }

        // Non-empty database.
        insert_test_data(&mut env, db);
        let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
        // Before db start.
        assert_eq!(
            cursor_get_less_than_or_equal_to(&cursor, b"0").unwrap(),
            None
        );
        // Equal.
        assert_eq!(
            cursor_get_less_than_or_equal_to(&cursor, b"3")
                .unwrap()
                .unwrap()
                .0,
            b"3"
        );
        // Less than.
        assert_eq!(
            cursor_get_less_than_or_equal_to(&cursor, b"4")
                .unwrap()
                .unwrap()
                .0,
            b"3"
        );
        // Past db end.
        assert_eq!(
            cursor_get_less_than_or_equal_to(&cursor, b"6")
                .unwrap()
                .unwrap()
                .0,
            b"5"
        );
    }

    #[test]
    fn test_cursor_less_than() {
        let (_temp_dir, mut env, db) = test_database();

        // Empty database.
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            assert_eq!(cursor_get_less_than(&cursor, b"6").unwrap(), None);
        }

        // Non-empty database.
        insert_test_data(&mut env, db);
        let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
        // Before db start.
        assert_eq!(cursor_get_less_than(&cursor, b"0").unwrap(), None);
        // Equal element should be skipped.
        assert_eq!(
            cursor_get_less_than(&cursor, b"3").unwrap().unwrap().0,
            b"1"
        );
        // Less than.
        assert_eq!(
            cursor_get_less_than(&cursor, b"4").unwrap().unwrap().0,
            b"3"
        );
        // Past db end.
        assert_eq!(
            cursor_get_less_than(&cursor, b"6").unwrap().unwrap().0,
            b"5"
        );
    }

    #[test]
    fn test_raw_iterator() {
        let (_temp_dir, mut env, db) = test_database();

        // Empty database.
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Unbounded, true)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![]);
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Unbounded, false)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![]);
        }

        // Non-empty database.
        insert_test_data(&mut env, db);
        // Included ascending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Included(b"3"), true)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"3", b"5"]);
        }
        // Excluded ascending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Excluded(b"3"), true)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"5"]);
        }
        // Unbounded ascending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Unbounded, true)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"1", b"3", b"5"]);
        }
        // Included descending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Included(b"3"), false)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"3", b"1"]);
        }
        // Excluded descending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Excluded(b"3"), false)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"1"]);
        }
        // Unbounded descending
        {
            let cursor = env.txn_mut().unwrap().open_ro_cursor(db).unwrap();
            let items = RawIterator::new(cursor, Bound::Unbounded, false)
                .unwrap()
                .map(|result| result.map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(items, vec![b"5", b"3", b"1"]);
        }
    }
}
