use dozer_types::types::{Field, Record};

mod groupby;
mod operators;


pub trait Aggregator {
    fn get_type(&self) -> u8;
    fn insert(&self, curr_state: Option<&[u8]>, new: &Record) -> anyhow::Result<Vec<u8>>;
    fn update(&self, curr_state: Option<&[u8]>, old: &Record, new: &Record) -> anyhow::Result<Vec<u8>>;
    fn delete(&self, curr_state: Option<&[u8]>, old: &Record) -> anyhow::Result<Vec<u8>>;
    fn get_value(&self, f: &[u8]) -> Field;
}