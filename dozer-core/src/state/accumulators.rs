use bytemuck::from_bytes;
use dozer_shared::types::{Field, Record};
use dozer_shared::types::Field::{ Int };
use crate::state::{Aggregator, StateStoreError, StateStoreErrorType};

const IntegerSumAggregatorId : u8  = 0x01;

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
        IntegerSumAggregatorId
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


        Ok(Vec::from(1_i64.to_ne_bytes()))
    }

    fn delete(&self, curr_state: Option<&[u8]>, old: &Record) -> Result<Option<Vec<u8>>, StateStoreError> {

        Ok(Some(Vec::from((1_i64).to_ne_bytes())))
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }


}