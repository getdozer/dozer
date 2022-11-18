use lmdb::{Cursor, RoCursor};
use lmdb_sys::{MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_PREV, MDB_SET_RANGE};

enum CacheIteratorState<'a> {
    First {
        starting_key: Option<&'a [u8]>,
        ascending: bool,
    },
    NotFirst {
        ascending: bool,
    },
}

pub struct CacheIterator<'a> {
    cursor: &'a RoCursor<'a>,
    state: CacheIteratorState<'a>,
}

impl<'a> Iterator for CacheIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match self.state {
            CacheIteratorState::First {
                starting_key,
                ascending,
            } => {
                let res = match starting_key {
                    Some(starting_key) => {
                        if ascending {
                            match self.cursor.get(Some(starting_key), None, MDB_SET_RANGE) {
                                Ok((key, value)) => Ok((key, value)),
                                Err(lmdb::Error::NotFound) => {
                                    self.cursor.get(None, None, MDB_FIRST)
                                }

                                Err(e) => Err(e),
                            }
                        } else {
                            match self.cursor.get(Some(starting_key), None, MDB_SET_RANGE) {
                                Ok((key, value)) => {
                                    if key == Some(starting_key) {
                                        Ok((key, value))
                                    } else {
                                        self.cursor.get(None, None, MDB_PREV)
                                    }
                                }
                                Err(lmdb::Error::NotFound) => self.cursor.get(None, None, MDB_LAST),
                                Err(e) => Err(e),
                            }
                        }
                    }
                    None => {
                        if ascending {
                            self.cursor.get(None, None, MDB_FIRST)
                        } else {
                            self.cursor.get(None, None, MDB_LAST)
                        }
                    }
                };
                self.state = CacheIteratorState::NotFirst { ascending };
                res
            }
            CacheIteratorState::NotFirst { ascending } => {
                if ascending {
                    self.cursor.get(None, None, MDB_NEXT)
                } else {
                    self.cursor.get(None, None, MDB_PREV)
                }
            }
        };

        match res {
            Ok((key, val)) => key.map(|key| (key, val)),
            Err(_e) => None,
        }
    }
}
impl<'a> CacheIterator<'a> {
    pub fn new(cursor: &'a RoCursor, starting_key: Option<&'a [u8]>, ascending: bool) -> Self {
        CacheIterator {
            cursor,
            state: CacheIteratorState::First {
                starting_key,
                ascending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use lmdb::{Transaction, WriteFlags};

    use crate::cache::lmdb::{
        utils::{init_db, init_env},
        CacheOptions,
    };

    use super::CacheIterator;

    #[test]
    fn test_cache_iterator() {
        let options = CacheOptions::default();
        let env = init_env(&options).unwrap();
        let db = init_db(&env, None, &options, true).unwrap();

        // Insert test data.
        let mut txn = env.begin_rw_txn().unwrap();
        for key in [
            b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
        ] {
            txn.put(db, key, &[], WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();

        // Create testing cursor and utility function.
        let txn = env.begin_ro_txn().unwrap();
        let cursor = txn.open_ro_cursor(db).unwrap();
        let check = |starting_key, ascending, expected: Vec<&'static [u8]>| {
            let actual = CacheIterator::new(&cursor, starting_key, ascending)
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        };

        // Test ascending from start.
        check(
            None,
            true,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test ascending from start using the same cursor again.
        check(
            None,
            true,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test descending from last.
        check(
            None,
            false,
            vec![
                b"cc", b"cb", b"ca", b"bc", b"bb", b"ba", b"ac", b"ab", b"aa",
            ],
        );

        // Test descending from last using the same cursor again.
        check(
            None,
            false,
            vec![
                b"cc", b"cb", b"ca", b"bc", b"bb", b"ba", b"ac", b"ab", b"aa",
            ],
        );

        // Test ascending from existing key.
        let starting_key = b"ba".to_vec();
        check(
            Some(&starting_key),
            true,
            vec![b"ba", b"bb", b"bc", b"ca", b"cb", b"cc"],
        );

        // Test ascending from non existing key.
        let starting_key = b"00".to_vec();
        check(
            Some(&starting_key),
            true,
            vec![
                b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc",
            ],
        );

        // Test descending from existing key.
        let starting_key = b"bc".to_vec();
        check(
            Some(&starting_key),
            false,
            vec![b"bc", b"bb", b"ba", b"ac", b"ab", b"aa"],
        );

        // Test ascending from non-existing key.
        let starting_key = b"ad".to_vec();
        check(
            Some(&starting_key),
            true,
            vec![b"ba", b"bb", b"bc", b"ca", b"cb", b"cc"],
        );

        // Test descending from non-existing key.
        let starting_key = b"bd".to_vec();
        check(
            Some(&starting_key),
            false,
            vec![b"bc", b"bb", b"ba", b"ac", b"ab", b"aa"],
        );

        // Test descending from key past db end.
        let starting_key = b"dd".to_vec();
        check(
            Some(&starting_key),
            false,
            vec![
                b"cc", b"cb", b"ca", b"bc", b"bb", b"ba", b"ac", b"ab", b"aa",
            ],
        );
    }
}
