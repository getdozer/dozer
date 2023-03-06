use crate::pipeline::errors::PipelineError;
use dozer_types::types::Record;
use hashbrown::HashMap;
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
        record_map: &mut HashMap<Record, u8>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match (self.op, self.quantifier) {
            (SetOperator::Union, SetQuantifier::All) => Ok(vec![(action, record.clone())]),
            (SetOperator::Union, SetQuantifier::None) => {
                self.execute_union(action, record, record_map)
            }
            _ => Err(PipelineError::InvalidOperandType(self.op.to_string())),
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: &Record,
        record_map: &mut HashMap<Record, u8>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match action {
            SetAction::Insert => self.union_insert(action, record, record_map),
            SetAction::Delete => self.union_delete(action, record, record_map),
        }
    }

    fn union_insert(
        &self,
        action: SetAction,
        record: &Record,
        record_map: &mut HashMap<Record, u8>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let _count = self.update_map(record, 1, false, record_map);
        if _count == 1 {
            Ok(vec![(action, record.to_owned())])
        } else {
            Ok(vec![])
        }
    }

    fn union_delete(
        &self,
        action: SetAction,
        record: &Record,
        record_map: &mut HashMap<Record, u8>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let _count = self.update_map(record, 1, true, record_map);
        if _count == 0 {
            Ok(vec![(action, record.to_owned())])
        } else {
            Ok(vec![])
        }
    }

    fn update_map(
        &self,
        record: &Record,
        val_delta: u8,
        decr: bool,
        record_map: &mut HashMap<Record, u8>,
    ) -> u8 {
        let get_prev_count = record_map.get(record);
        let prev_count = match get_prev_count {
            Some(v) => *v,
            None => 0_u8,
        };
        let mut new_count = prev_count;
        if decr {
            new_count = new_count.wrapping_sub(val_delta);
        } else {
            new_count = new_count.wrapping_add(val_delta);
        }
        if new_count < 1 {
            record_map.remove(record);
        } else {
            record_map.insert(record.clone(), new_count);
        }
        new_count
    }
}
