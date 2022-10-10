use galil_seiferas::gs_find;
use lmdb::{Cursor, RoCursor};

pub struct CacheIterator<'a> {
    cursor: &'a RoCursor<'a>,
    starting_key: Option<Vec<u8>>,
    value_to_compare: Option<Vec<u8>>,
    ascending: bool,
    first: bool,
}

impl<'a> Iterator for CacheIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let key: Option<&[u8]> = if self.starting_key.is_none() {
            None
        } else {
            Some(self.starting_key.as_ref().unwrap())
        };

        if self.first == true {
            self.first = false;
        }
        let next_op = Self::get_next_op(self.starting_key.is_some(), self.ascending, self.first);
        let res = self.cursor.get(key, None, next_op.get_value());
        match res {
            Ok((key, val)) => match key {
                Some(key) => {
                    match self.value_to_compare {
                        Some(ref value_to_compare) => {
                            // TODO: find a better implementation
                            // Find for partial matches if iterating on a query
                            if let Some(_idx) = gs_find(key, value_to_compare) {
                                Some(val.to_vec())
                            } else {
                                None
                            }
                        }
                        None => Some(val.to_vec()),
                    }
                }
                None => None,
            },
            Err(e) => {
                println!("Error in cursor: {:?}", e);
                None
            }
        }
    }
}
impl<'a> CacheIterator<'a> {
    pub fn new(
        cursor: &'a RoCursor,
        starting_key: Option<Vec<u8>>,
        value_to_compare: Option<Vec<u8>>,
        ascending: bool,
    ) -> Self {
        CacheIterator {
            cursor,
            starting_key,
            value_to_compare,
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

enum NextOp {
    First,
    Last,
    Next,
    Prev,
}
impl NextOp {
    pub fn get_value(&self) -> u32 {
        match self {
            NextOp::First => MDB_FIRST,
            NextOp::Last => MDB_LAST,
            NextOp::Next => MDB_NEXT,
            NextOp::Prev => MDB_PREV,
        }
    }
}
