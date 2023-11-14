use crate::aggregation::aggregator::{update_val_map, Aggregator};
use crate::calculate_err;
use crate::errors::PipelineError;
use crate::errors::PipelineError::InvalidReturnType;
use dozer_sql_expression::aggregate::AggregateFunctionType::MaxValue;

use dozer_types::types::{Field, FieldType};
use std::collections::BTreeMap;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct MaxValueAggregator {
    current_state: BTreeMap<Field, u64>,
    return_state: BTreeMap<Field, Vec<Field>>,
    return_type: Option<FieldType>,
}

impl MaxValueAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MaxValueAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_val_map(
            old,
            1_u64,
            true,
            &mut self.current_state,
            &mut self.return_state,
        )?;
        get_max_value(&self.current_state, &self.return_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_val_map(
            new,
            1_u64,
            false,
            &mut self.current_state,
            &mut self.return_state,
        )?;
        get_max_value(&self.current_state, &self.return_state, self.return_type)
    }
}

fn get_max_value(
    field_map: &BTreeMap<Field, u64>,
    return_map: &BTreeMap<Field, Vec<Field>>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().max(), MaxValue).clone();

        match return_map.get(&val) {
            Some(v) => match v.first() {
                Some(v) => {
                    let value = v.clone();
                    Ok(value)
                }
                None => Err(InvalidReturnType(format!("{:?}", return_type))),
            },
            None => Err(InvalidReturnType(format!("{:?}", return_type))),
        }
    }
}
