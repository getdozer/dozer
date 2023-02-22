use crate::pipeline::errors::PipelineError;
use crate::{deserialize, try_unwrap};
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_types::parking_lot::RwLockWriteGuard;
use dozer_types::types::Record;
use lmdb::Database;
use sqlparser::ast::{SetOperator, SetQuantifier};

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum SetAction {
    Insert,
    Delete,
    // Update,
}

#[derive(Clone, Debug)]
pub struct SetOperation {
    pub op: SetOperator,

    pub quantifier: SetQuantifier,
}

impl SetOperation {
    pub fn _new(op: SetOperator) -> Self {
        Self {
            op,
            quantifier: SetQuantifier::None,
        }
    }

    pub fn execute(
        &self,
        action: SetAction,
        record: &Record,
        database: &Database,
        txn: &SharedTransaction,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match (self.op, self.quantifier) {
            (SetOperator::Union, SetQuantifier::All) => Ok(vec![(action, record.clone())]),
            (SetOperator::Union, SetQuantifier::None) => {
                self.execute_union(action, record, database, txn)
            }
            _ => Err(PipelineError::InvalidOperandType(self.op.to_string())),
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: &Record,
        database: &Database,
        txn: &SharedTransaction,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let lookup_key = record.get_values_hash().to_be_bytes();
        let write_txn = &mut txn.write();
        let mut _count: u8 = 0_u8;
        match action {
            SetAction::Insert => {
                _count = self.update_set_db(&lookup_key, 1, false, write_txn, *database);
                if _count == 1 {
                    Ok(vec![(action, record.to_owned())])
                } else {
                    Ok(vec![])
                }
            }
            SetAction::Delete => {
                _count = self.update_set_db(&lookup_key, 1, true, write_txn, *database);
                if _count == 0 {
                    Ok(vec![(action, record.to_owned())])
                } else {
                    Ok(vec![])
                }
            }
        }
    }

    fn update_set_db(
        &self,
        key: &[u8],
        val_delta: u8,
        decr: bool,
        ptx: &mut RwLockWriteGuard<LmdbExclusiveTransaction>,
        set_db: Database,
    ) -> u8 {
        let get_prev_count = try_unwrap!(ptx.get(set_db, key));
        let prev_count = match get_prev_count {
            Some(v) => u8::from_be_bytes(deserialize!(v)),
            None => 0_u8,
        };
        let mut new_count = prev_count;
        if decr {
            new_count = new_count.wrapping_sub(val_delta);
        } else {
            new_count = new_count.wrapping_add(val_delta);
        }
        if new_count < 1 {
            try_unwrap!(ptx.del(
                set_db,
                key,
                Option::from(prev_count.to_be_bytes().as_slice())
            ));
        } else {
            try_unwrap!(ptx.put(set_db, key, new_count.to_be_bytes().as_slice()));
        }
        new_count
    }
}
