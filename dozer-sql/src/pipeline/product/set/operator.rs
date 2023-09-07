use super::record_map::{CountingRecordMap, CountingRecordMapEnum};
use crate::pipeline::errors::PipelineError;
use dozer_core::processor_record::ProcessorRecord;
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
        record: ProcessorRecord,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, PipelineError> {
        match (self.op, self.quantifier) {
            (SetOperator::Union, SetQuantifier::All) => Ok(vec![(action, record)]),
            (SetOperator::Union, SetQuantifier::None) => {
                self.execute_union(action, record, record_map)
            }
            _ => Err(PipelineError::InvalidOperandType(self.op.to_string())),
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: ProcessorRecord,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, PipelineError> {
        match action {
            SetAction::Insert => self.union_insert(action, record, record_map),
            SetAction::Delete => self.union_delete(action, record, record_map),
        }
    }

    fn union_insert(
        &self,
        action: SetAction,
        record: ProcessorRecord,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, PipelineError> {
        let _count = self.update_map(record.clone(), false, record_map);
        if _count == 1 {
            Ok(vec![(action, record)])
        } else {
            Ok(vec![])
        }
    }

    fn union_delete(
        &self,
        action: SetAction,
        record: ProcessorRecord,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, PipelineError> {
        let _count = self.update_map(record.clone(), true, record_map);
        if _count == 0 {
            Ok(vec![(action, record)])
        } else {
            Ok(vec![])
        }
    }

    fn update_map(
        &self,
        record: ProcessorRecord,
        decr: bool,
        record_map: &mut CountingRecordMapEnum,
    ) -> u64 {
        if decr {
            record_map.remove(&record);
        } else {
            record_map.insert(&record);
        }

        record_map.estimate_count(&record)
    }
}
