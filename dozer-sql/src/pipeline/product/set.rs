use std::borrow::BorrowMut;
use std::collections::HashMap;
use lmdb::Database;
use dozer_types::types::Record;
use crate::pipeline::errors::{PipelineError, SetError};
use sqlparser::ast::{Select, SetOperator, SetQuantifier};
use dozer_core::node::PortHandle;
use dozer_core::record_store::RecordReader;
use dozer_core::storage::common::Seek;
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::parking_lot::RwLockWriteGuard;
use crate::pipeline::errors::SetError::HistoryUnavailable;
use crate::{deserialize, deserialize_u8, try_unwrap};

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum SetAction {
    Insert,
    Delete,
    // Update,
}

#[derive(Clone, Debug)]
pub struct SetOperation {
    pub op: SetOperator,
    pub left: Select,
    pub right: Select,
    pub quantifier: SetQuantifier,
}

impl SetOperation {
    pub fn _new(
        op: SetOperator,
        left: Select,
        right: Select,
    ) -> Self {
        Self {
            op,
            left,
            right,
            quantifier: SetQuantifier::None,
        }
    }

    pub fn execute(
        &self,
        action: SetAction,
        record: &Record,
        database: &Database,
        txn: &mut PrefixTransaction,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match (self.op, self.quantifier ) {
            (SetOperator::Union, SetQuantifier::All) => {
                Ok(vec![(action, record.clone())])
            },
            (SetOperator::Union, SetQuantifier::None) => {
                self.execute_union(action, record, database, txn)
            },
            _ => {
                Err(PipelineError::InvalidOperandType(self.op.to_string()))
            }
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: &Record,
        database: &Database,
        txn: &mut PrefixTransaction,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let lookup_key = record.get_values_hash().to_be_bytes();
        match action {
            SetAction::Insert => {
                self.update_set_db(&lookup_key, 1, false, txn, *database);
            }
            SetAction::Delete => {
                self.update_set_db(&lookup_key, 1, true, txn, *database);
            }
        }
        let curr_count = self.get_occurrence(&lookup_key, txn, *database)?;
        if curr_count == 1 {
            Ok(vec![(action, record.to_owned())])
        }
        else {
            Ok(vec![])
        }
    }

    fn update_set_db(
        &self,
        key: &[u8],
        val_delta: u8,
        decr: bool,
        ptx: &mut PrefixTransaction,
        set_db: Database,
    ) {
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
            try_unwrap!(ptx.del(set_db, key, Option::from(prev_count.to_be_bytes().as_slice())));
        } else {
            try_unwrap!(ptx.put(set_db, key, new_count.to_be_bytes().as_slice()));
        }
    }

    fn get_occurrence(
        &self,
        key: &[u8],
        ptx: &mut PrefixTransaction,
        set_db: Database,
    ) -> Result<u8, PipelineError> {
        let get_count = ptx.get(set_db, key);
        if get_count.is_ok() {
            let count = deserialize_u8!(try_unwrap!(get_count));
            Ok(count)
        } else {
            Ok(0_u8)
        }
    }
}
