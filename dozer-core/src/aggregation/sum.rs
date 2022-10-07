use anyhow::anyhow;
use dozer_types::types::{Field, FieldType, Record};
use dozer_types::types::Field::{ Int };
use crate::aggregation::Aggregator;

const INTEGER_SUM_AGGREGATOR_ID: u8  = 0x01;

#[derive(Clone)]
pub struct IntegerSumAggregator {
}

impl IntegerSumAggregator {
    pub fn new() -> Self {
        Self { }
    }
}


impl Aggregator for IntegerSumAggregator {

    fn get_return_type(&self) -> FieldType { FieldType::Int }

    fn get_type(&self) -> u8 {
        INTEGER_SUM_AGGREGATOR_ID
    }

    fn insert(&self, curr_state: Option<&[u8]>, new: &Field) -> anyhow::Result<Vec<u8>> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };
        let curr = match &new {
            Int(i) => { i }
            _ => {return  Err(anyhow!("Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev + *curr).to_ne_bytes()))
    }

    fn update(&self, curr_state: Option<&[u8]>, old: &Field, new: &Field) -> anyhow::Result<Vec<u8>> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };

        let curr_del = match &old {
            Int(i) => { i }
            _ => {return  Err(anyhow!("Invalid data type".to_string())); }
        };
        let curr_added = match &new {
            Int(i) => { i }
            _ => {return  Err(anyhow!("Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
    }

    fn delete(&self, curr_state: Option<&[u8]>, old: &Field) -> anyhow::Result<Vec<u8>> {

        let prev = if curr_state.is_none() {0_i64} else { i64::from_ne_bytes(curr_state.unwrap().try_into().unwrap()) };
        let curr = match &old {
            Int(i) => { i }
            _ => {return  Err(anyhow!("Invalid data type".to_string())); }
        };

        Ok(Vec::from((prev - *curr).to_ne_bytes()))
    }


    fn get_value(&self, f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }


}