use bytemuck::from_bytes;
use dozer_shared::types::Field;
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

    fn get_state_size(&self) -> Option<u32> {
        Some(8)
    }


    fn insert(&self, bool: initial, prev: &mut [u8], curr: Field) -> Result<(), StateStoreError> {

       Ok(())
    }

    fn delete(&self, bool: initial, prev: &mut [u8], curr: Field) -> Result<(), StateStoreError> {

        Ok(())
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(*from_bytes::<i64>(f))
    }


}