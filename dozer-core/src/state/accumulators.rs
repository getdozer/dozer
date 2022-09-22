use bytemuck::from_bytes;
use dozer_shared::types::Field;
use dozer_shared::types::Field::{ Int };
use crate::state::{Accumulator, StateStoreError};
use crate::state::StateStoreErrorType::AccumulationError;

const IntegerSumAccumulatorId : u8  = 0x01;

pub struct IntegerSumAccumulator {

}

impl Accumulator for IntegerSumAccumulator {

    fn get_type(&self) -> u8 {
        IntegerSumAccumulatorId
    }

    fn accumulate(&self, prev: Option<&[u8]>, curr: Field) -> Result<Vec<u8>, StateStoreError> {

        let v: Option<i64> = match curr {
            Int(s) => { Some(s) }
            _ => { None}
        };
        if v.is_none() {
            return Err(StateStoreError::new(AccumulationError, "Invalid data type".to_string()));
        }

        if v.is_none() {
            Ok(Vec::<u8>::from(v.unwrap().to_ne_bytes()))
        }
        else {
            let tot = from_bytes::<i64>(prev.unwrap()) + v.unwrap();
            Ok(Vec::<u8>::from(tot.to_ne_bytes()))
        }
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(*from_bytes::<i64>(f))
    }


}