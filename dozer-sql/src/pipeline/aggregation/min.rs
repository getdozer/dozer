use crate::pipeline::aggregation::aggregator::{update_map, Aggregator};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Min;
use crate::{calculate_err, calculate_err_field};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct MinAggregator {
    current_state: BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
}

impl MinAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MinAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_map(old, 1_u64, true, &mut self.current_state);
        get_min(&self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_map(new, 1_u64, false, &mut self.current_state);
        get_min(&self.current_state, self.return_type)
    }
}

fn get_min(
    field_map: &BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().min(), Min).clone();
        match return_type {
            Some(FieldType::UInt) => Ok(Field::UInt(calculate_err_field!(val.to_uint(), Min, val))),
            Some(FieldType::Int) => Ok(Field::Int(calculate_err_field!(val.to_int(), Min, val))),
            Some(FieldType::Float) => Ok(Field::Float(OrderedFloat::from(calculate_err_field!(
                val.to_float(),
                Min,
                val
            )))),
            Some(FieldType::Decimal) => Ok(Field::Decimal(calculate_err_field!(
                val.to_decimal(),
                Min,
                val
            ))),
            Some(FieldType::Timestamp) => Ok(Field::Timestamp(calculate_err_field!(
                val.to_timestamp()?,
                Min,
                val
            ))),
            Some(FieldType::Date) => {
                Ok(Field::Date(calculate_err_field!(val.to_date()?, Min, val)))
            }
            Some(not_supported_return_type) => {
                Err(PipelineError::InternalExecutionError(InvalidType(format!(
                    "Not supported return type {} for {}",
                    not_supported_return_type, Min
                ))))
            }
            None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported None return type for {}",
                Min
            )))),
        }
    }
}
