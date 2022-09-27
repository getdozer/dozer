use dozer_types::types::{Field, Record};
use dozer_types::types::Field::{ Int };
use crate::aggregation::Aggregator;
use crate::state::{StateStoreError, StateStoreErrorType};

const INTEGER_SUM_AGGREGATOR_ID: u8  = 0x01;

pub struct IntegerSumAggregator {
    input_idx: usize
}

impl IntegerSumAggregator {
    pub fn new(input_idx: usize) -> Self {
        Self { input_idx }
    }
}


impl Aggregator for IntegerSumAggregator {

    fn get_type(&self) -> u8 {
        INTEGER_SUM_AGGREGATOR_ID
    }

    fn insert(&self, curr_state: Option<&[u8]>, new: &Record) -> Result<Vec<u8>, StateStoreError> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };
        let curr = match &new.values[self.input_idx] {
            Int(i) => { i }
            _ => {return  Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev + *curr).to_ne_bytes()))
    }

    fn update(&self, curr_state: Option<&[u8]>, old: &Record, new: &Record) -> Result<Vec<u8>, StateStoreError> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };

        let curr_del = match &old.values[self.input_idx] {
            Int(i) => { i }
            _ => {return  Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid data type".to_string())); }
        };
        let curr_added = match &new.values[self.input_idx] {
            Int(i) => { i }
            _ => {return  Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
    }

    fn delete(&self, curr_state: Option<&[u8]>, old: &Record) -> Result<Vec<u8>, StateStoreError> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };
        let curr = match &old.values[self.input_idx] {
            Int(i) => { i }
            _ => {return  Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev - *curr).to_ne_bytes()))
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }


}