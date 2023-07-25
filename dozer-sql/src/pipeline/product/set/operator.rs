use crate::pipeline::errors::PipelineError;
use bloom::{CountingBloomFilter, ASMS};
use dozer_core::processor_record::ProcessorRecordRef;
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
        record: ProcessorRecordRef,
        record_map: &mut CountingBloomFilter,
    ) -> Result<Vec<(SetAction, ProcessorRecordRef)>, PipelineError> {
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
        record: ProcessorRecordRef,
        record_map: &mut CountingBloomFilter,
    ) -> Result<Vec<(SetAction, ProcessorRecordRef)>, PipelineError> {
        match action {
            SetAction::Insert => self.union_insert(action, record, record_map),
            SetAction::Delete => self.union_delete(action, record, record_map),
        }
    }

    fn union_insert(
        &self,
        action: SetAction,
        record: ProcessorRecordRef,
        record_map: &mut CountingBloomFilter,
    ) -> Result<Vec<(SetAction, ProcessorRecordRef)>, PipelineError> {
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
        record: ProcessorRecordRef,
        record_map: &mut CountingBloomFilter,
    ) -> Result<Vec<(SetAction, ProcessorRecordRef)>, PipelineError> {
        let _count = self.update_map(record.clone(), true, record_map);
        if _count == 0 {
            Ok(vec![(action, record)])
        } else {
            Ok(vec![])
        }
    }

    fn update_map(
        &self,
        record: ProcessorRecordRef,
        decr: bool,
        record_map: &mut CountingBloomFilter,
    ) -> u32 {
        if decr {
            record_map.remove(&record);
        } else {
            record_map.insert(&record);
        }

        record_map.estimate_count(&record)
    }
}
