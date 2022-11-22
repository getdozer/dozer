use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::Int;
use crate::pipeline::errors::PipelineError;

pub struct CountAggregator {}

impl CountAggregator {
    const _AGGREGATOR_ID: u8 = 0x02;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Int
    }

    pub(crate) fn _get_type() -> u8 {
        CountAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        _new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        Ok(Vec::from((prev + 1).to_ne_bytes()))
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        _old: &Field,
        _new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        Ok(Vec::from((prev).to_ne_bytes()))
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        _old: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        Ok(Vec::from((prev - 1).to_ne_bytes()))
    }

    pub(crate) fn get_value(f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }
}
