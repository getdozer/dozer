use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::aggregation::error::AggregatorError;
use crate::pipeline::aggregation::error::AggregatorError::InvalidOperandType;
use dozer_types::types::Field::Int;
use dozer_types::types::{Field, FieldType};

const INTEGER_SUM_AGGREGATOR_ID: u8 = 0x01;

#[derive(Clone)]
pub struct IntegerSumAggregator {}

impl Default for IntegerSumAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl IntegerSumAggregator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Aggregator for IntegerSumAggregator {
    fn get_return_type(&self, _input_type: FieldType) -> FieldType {
        FieldType::Int
    }

    fn get_type(&self) -> u8 {
        INTEGER_SUM_AGGREGATOR_ID
    }

    fn insert(&self, curr_state: Option<&[u8]>, new: &Field) -> Result<Vec<u8>, AggregatorError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev + *curr).to_ne_bytes()))
    }

    fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, AggregatorError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr_del = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };
        let curr_added = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
    }

    fn delete(&self, curr_state: Option<&[u8]>, old: &Field) -> Result<Vec<u8>, AggregatorError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr).to_ne_bytes()))
    }

    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }
}
