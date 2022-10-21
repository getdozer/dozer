use crate::pipeline::aggregation::error::AggregatorError;
use dozer_types::types::{Field, FieldType};
use dyn_clone::DynClone;

pub trait Aggregator: DynClone + Send + Sync {
    fn get_return_type(&self, from: FieldType) -> FieldType;
    fn get_type(&self) -> u8;
    fn insert(&self, curr_state: Option<&[u8]>, new: &Field) -> Result<Vec<u8>, AggregatorError>;
    fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, AggregatorError>;
    fn delete(&self, curr_state: Option<&[u8]>, old: &Field) -> Result<Vec<u8>, AggregatorError>;
    fn get_value(&self, f: &[u8]) -> Field;
}

dyn_clone::clone_trait_object!(Aggregator);
