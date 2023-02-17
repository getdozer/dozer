use std::marker::PhantomData;

use dozer_storage::lmdb::Cursor;
use dozer_storage::lmdb_sys::{
    MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_NEXT_NODUP, MDB_PREV, MDB_PREV_NODUP, MDB_SET_RANGE,
};

use crate::cache::expression::SortDirection;

#[derive(Debug, Clone)]
pub enum KeyEndpoint {
    Including(Vec<u8>),
    Excluding(Vec<u8>),
}

impl KeyEndpoint {
    pub fn key(&self) -> &[u8] {
        match self {
            KeyEndpoint::Including(key) => key,
            KeyEndpoint::Excluding(key) => key,
        }
    }
}

enum CacheIteratorState {
    First {
        starting_key: Option<KeyEndpoint>,
        direction: SortDirection,
    },
    NotFirst {
        direction: SortDirection,
    },
}

pub struct CacheIterator<'txn, C: Cursor<'txn>> {
    cursor: C,
    state: CacheIteratorState,
    _marker: PhantomData<fn() -> &'txn ()>,
}

impl<'txn, C: Cursor<'txn>> Iterator for CacheIterator<'txn, C> {
    type Item = (&'txn [u8], &'txn [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match &self.state {
            CacheIteratorState::First {
                starting_key,
                direction,
            } => {
                let res = match starting_key {
                    Some(starting_key) => match direction {
                        SortDirection::Ascending => {
                            match self
                                .cursor
                                .get(Some(starting_key.key()), None, MDB_SET_RANGE)
                            {
                                Ok((key, value)) => {
                                    if key == Some(starting_key.key())
                                        && matches!(starting_key, KeyEndpoint::Excluding(_))
                                    {
                                        self.cursor.get(None, None, MDB_NEXT_NODUP)
                                    } else {
                                        Ok((key, value))
                                    }
                                }
                                Err(dozer_storage::lmdb::Error::NotFound) => {
                                    return None;
                                }

                                Err(e) => Err(e),
                            }
                        }
                        SortDirection::Descending => {
                            match self
                                .cursor
                                .get(Some(starting_key.key()), None, MDB_SET_RANGE)
                            {
                                Ok((key, value)) => {
                                    if key == Some(starting_key.key())
                                        && matches!(starting_key, KeyEndpoint::Including(_))
                                    {
                                        Ok((key, value))
                                    } else {
                                        self.cursor.get(None, None, MDB_PREV_NODUP)
                                    }
                                }
                                Err(dozer_storage::lmdb::Error::NotFound) => {
                                    self.cursor.get(None, None, MDB_LAST)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    },
                    None => match direction {
                        SortDirection::Ascending => self.cursor.get(None, None, MDB_FIRST),
                        SortDirection::Descending => self.cursor.get(None, None, MDB_LAST),
                    },
                };
                self.state = CacheIteratorState::NotFirst {
                    direction: *direction,
                };
                res
            }
            CacheIteratorState::NotFirst { direction } => match direction {
                SortDirection::Ascending => self.cursor.get(None, None, MDB_NEXT),
                SortDirection::Descending => self.cursor.get(None, None, MDB_PREV),
            },
        };

        match res {
            Ok((key, val)) => key.map(|key| (key, val)),
            Err(_e) => None,
        }
    }
}
impl<'txn, C: Cursor<'txn>> CacheIterator<'txn, C> {
    pub fn new(cursor: C, starting_key: Option<KeyEndpoint>, direction: SortDirection) -> Self {
        CacheIterator {
            cursor,
            state: CacheIteratorState::First {
                starting_key,
                direction,
            },
            _marker: PhantomData::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb::{DatabaseFlags, Transaction, WriteFlags};

    use crate::cache::{
        expression::SortDirection,
        lmdb::utils::{init_env, CacheOptions},
    };

    use super::{CacheIterator, KeyEndpoint};

    #[test]
    fn test_cache_iterator() {
        let options = CacheOptions::default();
        let mut env = init_env(&options).unwrap().0;
        let db = env
            .create_database(None, Some(DatabaseFlags::DUP_SORT))
            .unwrap();
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();

        // Insert test data.
        let txn = txn.txn_mut();
        for key in [
            b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
        ] {
            txn.put(db, key, &[], WriteFlags::empty()).unwrap();
        }

        // Create testing cursor and utility function.
        let check = |starting_key, direction, expected: Vec<&'static [u8]>| {
            let cursor = txn.open_ro_cursor(db).unwrap();
            let actual = CacheIterator::new(cursor, starting_key, direction)
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        };

        // Test ascending from start.
        check(
            None,
            SortDirection::Ascending,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test descending from last.
        check(
            None,
            SortDirection::Descending,
            vec![
                b"cc", b"cb", b"ca", b"bc", b"bb", b"ba", b"ac", b"ab", b"aa",
            ],
        );

        // Test ascending from key before db start.
        let starting_key = b"a".to_vec();
        check(
            Some(KeyEndpoint::Excluding(starting_key)),
            SortDirection::Ascending,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test descending from key before db start.
        let starting_key = b"a".to_vec();
        check(
            Some(KeyEndpoint::Excluding(starting_key)),
            SortDirection::Descending,
            vec![],
        );

        // Test ascending from existing key.
        let starting_key = b"ba".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key.clone())),
            SortDirection::Ascending,
            vec![b"ba", b"bb", b"bc", b"ca", b"cb", b"cc"],
        );
        check(
            Some(KeyEndpoint::Excluding(starting_key)),
            SortDirection::Ascending,
            vec![b"bb", b"bc", b"ca", b"cb", b"cc"],
        );

        // Test ascending from non existing key.
        let starting_key = b"00".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key)),
            SortDirection::Ascending,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test descending from existing key.
        let starting_key = b"bc".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key.clone())),
            SortDirection::Descending,
            vec![b"bc", b"bb", b"ba", b"ac", b"ab", b"aa"],
        );
        check(
            Some(KeyEndpoint::Excluding(starting_key)),
            SortDirection::Descending,
            vec![b"bb", b"ba", b"ac", b"ab", b"aa"],
        );

        // Test ascending from non-existing key.
        let starting_key = b"ad".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key)),
            SortDirection::Ascending,
            vec![b"ba", b"bb", b"bc", b"ca", b"cb", b"cc"],
        );

        // Test descending from non-existing key.
        let starting_key = b"bd".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key)),
            SortDirection::Descending,
            vec![b"bc", b"bb", b"ba", b"ac", b"ab", b"aa"],
        );

        // Test descending from key past db end.
        let starting_key = b"dd".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key)),
            SortDirection::Descending,
            vec![
                b"cc", b"cb", b"ca", b"bc", b"bb", b"ba", b"ac", b"ab", b"aa",
            ],
        );

        // Test ascending from key past db end.
        let starting_key = b"dd".to_vec();
        check(
            Some(KeyEndpoint::Including(starting_key)),
            SortDirection::Ascending,
            vec![],
        );
    }
}
