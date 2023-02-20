use crate::pipeline::errors::PipelineError;
use dozer_core::node::PortHandle;
use dozer_types::types::Record;
use sqlparser::ast::{SetOperator, SetQuantifier};

#[derive(Clone, Debug, PartialEq, Eq)]
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
        _from_port: PortHandle,
        record: &Record,
        record_hash_map: &mut Vec<u64>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match (self.op, self.quantifier) {
            (SetOperator::Union, SetQuantifier::All) => Ok(vec![(action, record.clone())]),
            (SetOperator::Union, SetQuantifier::None) => {
                self.execute_union(action, record, record_hash_map)
            }
            _ => Err(PipelineError::InvalidOperandType(self.op.to_string())),
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: &Record,
        record_hash_map: &mut Vec<u64>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let mut output_records: Vec<(SetAction, Record)> = vec![];
        let lookup_key = record.get_values_hash();
        if !record_hash_map.contains(&lookup_key) {
            output_records.push((action, record.clone()));
            record_hash_map.push(lookup_key);
        }
        Ok(output_records)
    }
}
