use crate::pipeline::aggregation::aggregator::{update_map, Aggregator};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Avg;
use crate::{calculate_err_field, calculate_err_type};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType};
use num_traits::FromPrimitive;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct AvgAggregator {
    current_state: BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
}

impl AvgAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for AvgAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_map(old, 1_u64, true, &mut self.current_state);
        get_average(&self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_map(new, 1_u64, false, &mut self.current_state);
        get_average(&self.current_state, self.return_type)
    }
}

fn get_average(
    field_map: &BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    match return_type {
        Some(FieldType::UInt) => {
            if field_map.is_empty() {
                Ok(Field::UInt(0_u64))
            } else {
                let mut sum = 0_u64;
                let mut count = 0_u64;
                for (field, cnt) in field_map {
                    sum += calculate_err_field!(field.to_uint(), Avg, field);
                    count += *cnt;
                }
                Ok(Field::UInt(sum / count))
            }
        }
        Some(FieldType::Int) => {
            if field_map.is_empty() {
                Ok(Field::Int(0_i64))
            } else {
                let mut sum = 0_i64;
                let mut count = 0_i64;
                for (field, cnt) in field_map {
                    sum += calculate_err_field!(field.to_int(), Avg, field);
                    count += *cnt as i64;
                }
                Ok(Field::Int(sum / count))
            }
        }
        Some(FieldType::Float) => {
            if field_map.is_empty() {
                Ok(Field::Float(OrderedFloat::from(0_f64)))
            } else {
                let mut sum = 0_f64;
                let mut count = 0_f64;
                for (field, cnt) in field_map {
                    sum += calculate_err_field!(field.to_float(), Avg, field);
                    count += *cnt as f64;
                }
                Ok(Field::Float(OrderedFloat::from(sum / count)))
            }
        }
        Some(FieldType::Decimal) => {
            if field_map.is_empty() {
                Ok(Field::Decimal(calculate_err_type!(
                    Decimal::from_f64(0_f64),
                    Avg,
                    FieldType::Decimal
                )))
            } else {
                let mut sum =
                    calculate_err_type!(Decimal::from_f64(0_f64), Avg, FieldType::Decimal);
                let mut count =
                    calculate_err_type!(Decimal::from_f64(0_f64), Avg, FieldType::Decimal);
                for (field, cnt) in field_map {
                    sum += calculate_err_field!(field.to_decimal(), Avg, field);
                    count += calculate_err_field!(Decimal::from_u64(*cnt), Avg, field);
                }
                Ok(Field::Decimal(sum / count))
            }
        }
        Some(not_supported_return_type) => {
            Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported return type {} for {}",
                not_supported_return_type, Avg
            ))))
        }
        None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
            "Not supported None return type for {}",
            Avg
        )))),
    }
}
