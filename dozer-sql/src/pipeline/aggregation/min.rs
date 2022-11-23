use std::cmp::min;
use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::Int;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;

pub struct IntegerMinAggregator {}

impl IntegerMinAggregator {
    const _AGGREGATOR_ID: u8 = 0x03;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Int
    }

    pub(crate) fn _get_type() -> u8 {
        IntegerMinAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(curr_state: Option<&[u8]>, new: &Field) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("MIN".to_string()));
            }
        };

        Ok(Vec::from(min(prev, *curr).to_ne_bytes()))
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {

    }

    pub(crate) fn delete(curr_state: Option<&[u8]>, old: &Field) -> Result<Vec<u8>, PipelineError> {
        l
    }

    pub(crate) fn get_value(f: &[u8]) -> Field {

    }
}