use lmdb::{Cursor, RoCursor};
use log::debug;

pub struct CacheIterator<'a> {
    cursor: &'a RoCursor<'a>,
    starting_key: Option<&'a Vec<u8>>,
    ascending: bool,
    first: bool,
}

impl<'a> Iterator for CacheIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let key: Option<&[u8]> = if self.starting_key.is_none() {
            None
        } else {
            Some(self.starting_key.as_ref().unwrap())
        };

        if self.first {
            self.first = false;
        }
        let next_op = Self::get_next_op(self.starting_key.is_some(), self.ascending, self.first);
        let res = self.cursor.get(key, None, next_op.get_value());
        match res {
            Ok((key, val)) => key.map(|key| (key, val)),
            Err(e) => {
                debug!("Error in cursor: {:?}", e);
                None
            }
        }
    }
}
impl<'a> CacheIterator<'a> {
    pub fn new(cursor: &'a RoCursor, starting_key: Option<&'a Vec<u8>>, ascending: bool) -> Self {
        CacheIterator {
            cursor,
            starting_key,
            ascending,
            first: true,
        }
    }

    fn get_next_op(has_expression: bool, ascending: bool, first: bool) -> NextOp {
        let next_op = if first && has_expression {
            NextOp::First
        } else {
            NextOp::Next
        };

        if !ascending {
            match next_op {
                NextOp::First => NextOp::Last,
                NextOp::Last => NextOp::First,
                NextOp::Next => NextOp::Prev,
                NextOp::Prev => NextOp::Next,
            }
        } else {
            next_op
        }
    }
}

// http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
pub const MDB_FIRST: u32 = 0;
pub const MDB_LAST: u32 = 6;
pub const MDB_NEXT: u32 = 8;
pub const MDB_PREV: u32 = 12;
pub const MDB_SET_RANGE: u32 = 17;

enum NextOp {
    First,
    Last,
    Next,
    Prev,
}
impl NextOp {
    pub fn get_value(&self) -> u32 {
        match self {
            NextOp::First => MDB_SET_RANGE,
            NextOp::Last => MDB_LAST,
            NextOp::Next => MDB_NEXT,
            NextOp::Prev => MDB_PREV,
        }
    }
}

#[cfg(test)]
mod tests {
    use lmdb::{Transaction, WriteFlags};

    use crate::cache::lmdb::utils::{init_db, init_env};

    use super::CacheIterator;

    #[test]
    fn test_cache_iterator() {
        let env = init_env(true).unwrap();
        let db = init_db(&env, None).unwrap();

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
    }
}
