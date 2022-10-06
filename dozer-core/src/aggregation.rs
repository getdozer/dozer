use dozer_types::types::{Field, FieldType, Record};
use dyn_clone::DynClone;

pub mod groupby;
pub mod sum;
mod tests;



pub trait Aggregator : DynClone + Send + Sync {
    fn get_return_type(&self) -> FieldType;
    fn get_type(&self) -> u8;
    fn insert(&self, curr_state: Option<&[u8]>, new: &Field) -> anyhow::Result<Vec<u8>>;
    fn update(&self, curr_state: Option<&[u8]>, old: &Field, new: &Field) -> anyhow::Result<Vec<u8>>;
    fn delete(&self, curr_state: Option<&[u8]>, old: &Field) -> anyhow::Result<Vec<u8>>;
    fn get_value(&self, f: &[u8]) -> Field;
}

dyn_clone::clone_trait_object!(Aggregator);