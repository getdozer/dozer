use dozer_types::types::{Field, Record};
use crate::state::StateStoreError;

mod groupby;
mod operators;


pub trait Aggregator {
    fn get_type(&self) -> u8;
    fn insert(&self, curr_state: Option<&[u8]>, new: &Record) -> Result<Vec<u8>, StateStoreError>;
    fn update(&self, curr_state: Option<&[u8]>, old: &Record, new: &Record) -> Result<Vec<u8>, StateStoreError>;
    fn delete(&self, curr_state: Option<&[u8]>, old: &Record) -> Result<Vec<u8>, StateStoreError>;
    fn get_value(&self, f: &[u8]) -> Field;
}