use bytemuck::from_bytes;
use dozer_shared::types::{Field, Record};
use dozer_shared::types::Field::{ Int };
use crate::state::{Aggregator, StateStoreError, StateStoreErrorType};

const IntegerSumAggregatorId : u8  = 0x01;

pub struct IntegerSumAggregator {

}

impl IntegerSumAggregator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Aggregator for IntegerSumAggregator {

    fn get_type(&self) -> u8 {
        IntegerSumAggregatorId
    }

    fn insert(&self, prev: Option<&[u8]>, curr: &Record) -> Result<Vec<u8>, StateStoreError> {

      //  let r = if prev.is_none() {0_i64} else { (i64::from_ne_bytes(prev.unwrap().try_into().unwrap()) + 1) };

        let r = 0_i64;
        Ok(Vec::from(r.to_ne_bytes()))
    }

    fn delete(&self, prev: Option<&[u8]>, curr: &Record) -> Result<Option<Vec<u8>>, StateStoreError> {

      //  let r = if prev.is_none() { 0_i64.to_ne_bytes() } else { (from_bytes::<i64>(prev.unwrap()) + 1).to_ne_bytes() };

        let r = 0_i64;
        Ok(Some(Vec::from(r.to_ne_bytes())))
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }


}