use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::Field::Int;
use dozer_types::types::{Field, FieldType};

const COUNT_AGGREGATOR_ID: u8 = 0x02;

#[derive(Clone)]
pub struct CountAggregator {}

impl Default for CountAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl CountAggregator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Aggregator for CountAggregator {
    fn get_return_type(&self, _input_type: FieldType) -> FieldType {
        FieldType::Int
    }

    fn get_type(&self) -> u8 {
        COUNT_AGGREGATOR_ID
    }

    fn insert(&self, curr_state: Option<&[u8]>, _new: &Field) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        Ok(Vec::from((prev + 1).to_ne_bytes()))
    }

    fn update(
        &self,
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

    fn delete(&self, curr_state: Option<&[u8]>, _old: &Field) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        Ok(Vec::from((prev - 1).to_ne_bytes()))
    }

    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }
}
