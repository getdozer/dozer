use crate::pipeline::aggregation::aggregator::{update_map, Aggregator};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Max;
use crate::{calculate_err, calculate_err_field};
use dozer_core::errors::ExecutionError::InvalidType;

use crate::pipeline::expression::execution::ExpressionType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, SourceDefinition};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct MaxAggregator {
    current_state: BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
}

impl MaxAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MaxAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_map(old, 1_u64, true, &mut self.current_state);
        get_max(&self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_map(new, 1_u64, false, &mut self.current_state);
        get_max(&self.current_state, self.return_type)
    }
}

fn get_max(
    field_map: &BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().max(), Max).clone();
        match return_type {
            Some(FieldType::UInt) => Ok(Field::UInt(calculate_err_field!(val.to_uint(), Max, val))),
            Some(FieldType::Int) => Ok(Field::Int(calculate_err_field!(val.to_int(), Max, val))),
            Some(FieldType::Float) => Ok(Field::Float(OrderedFloat::from(calculate_err_field!(
                val.to_float(),
                Max,
                val
            )))),
            Some(FieldType::Decimal) => Ok(Field::Decimal(calculate_err_field!(
                val.to_decimal(),
                Max,
                val
            ))),
            Some(FieldType::Timestamp) => Ok(Field::Timestamp(calculate_err_field!(
                val.to_timestamp()?,
                Max,
                val
            ))),
            Some(FieldType::Date) => {
                Ok(Field::Date(calculate_err_field!(val.to_date()?, Max, val)))
            }
            Some(not_supported_return_type) => {
                Err(PipelineError::InternalExecutionError(InvalidType(format!(
                    "Not supported return type {not_supported_return_type} for {Max}"
                ))))
            }
            None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported None return type for {Max}"
            )))),
        }
    }
}
