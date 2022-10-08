use galil_seiferas::gs_find;
use lmdb::{Cursor, RoCursor};

pub struct CacheCursor<'a> {
    cursor: &'a RoCursor<'a>,
}
// http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
pub const MDB_FIRST: isize = 0;
pub const MDB_LAST: isize = 6;
pub const MDB_NEXT: isize = 8;
pub const MDB_PREV: isize = 12;

enum NextOp {
    FIRST = MDB_FIRST,
    LAST = MDB_LAST,
    NEXT = MDB_NEXT,
    PREV = MDB_PREV,
}

impl<'a> CacheCursor<'a> {
    pub fn new(cursor: &'a RoCursor) -> Self {
        CacheCursor { cursor }
    }
    pub fn get_records(
        &self,
        starting_key: Option<Vec<u8>>,
        field_to_compare: Option<Vec<u8>>,
        ascending: bool,
        no_of_rows: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let cursor = &self.cursor;
        let mut docs = vec![];
        let mut idx: usize = 0;

        let key: Option<&[u8]> = if starting_key.is_none() {
            None
        } else {
            Some(&starting_key.as_ref().unwrap())
        };

        loop {
            if idx >= no_of_rows {
                break;
            }
            let next_op = Self::get_next_op(starting_key.is_some(), ascending, idx) as u32;
            let res = cursor.get(key, None, next_op);
            match res {
                Ok((key, val)) => match key {
                    Some(key) => {
                        match field_to_compare {
                            Some(ref compared_field) => {
                                // Find for partial matches if iterating on a query
                                if let Some(_idx) = gs_find(key, &compared_field) {
                                    docs.push(val.to_vec());
                                } else {
                                    break;
                                }
                            }
                            None => {
                                docs.push(val.to_vec());
                            }
                        }
                    }
                    None => {
                        break;
                    }
                },
                Err(e) => {
                    println!("Error in cursor: {:?}", e);
                    break;
                }
            }
            idx += 1;
        }
        Ok(docs)
    }
    fn get_next_op(has_expression: bool, ascending: bool, idx: usize) -> NextOp {
        let next_op = if idx == 0 && has_expression {
            NextOp::FIRST
        } else {
            NextOp::NEXT
        };

        if !ascending {
            match next_op {
                NextOp::FIRST => NextOp::LAST,
                NextOp::LAST => NextOp::FIRST,
                NextOp::NEXT => NextOp::PREV,
                NextOp::PREV => NextOp::NEXT,
            }
        } else {
            next_op
        }
    }
}
