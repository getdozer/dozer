use std::cmp::max;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::{Float, Int};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;

pub struct IntegerMaxAggregator {}

impl IntegerMaxAggregator {
    const _AGGREGATOR_ID: u8 = 0x03;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Int
    }

    pub(crate) fn _get_type() -> u8 {
        IntegerMaxAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(curr_state: Option<&[u8]>, new: &Field) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("MAX".to_string()));
            }
        };

        Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
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