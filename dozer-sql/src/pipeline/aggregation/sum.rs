use std::collections::BTreeMap;
use num_traits::FromPrimitive;
use dozer_core::errors::ExecutionError::InvalidOperation;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::tonic::codegen::Body;
use crate::pipeline::aggregation::aggregator::{Aggregator, update_map};
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Sum;

pub struct SumAggregator {
    current_state: BTreeMap<Field, u64>,
}

impl SumAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
        }
    }
}

impl Aggregator for SumAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to update while deleting record: {} for {}", old, Sum.to_string()))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to update while inserting record: {} for {}", new, Sum.to_string()))))
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match old.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(old, 1_u64, true, &mut self.current_state);
                    get_sum(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, old, Sum.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", old, Sum.to_string())))),
        }
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match new.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(new, 1_u64, true, &mut self.current_state);
                    get_sum(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, new, Sum.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Sum.to_string())))),
        }
    }
}

fn get_sum(field_hash: &BTreeMap<Field, u64>, return_type: FieldType) -> Result<Field, PipelineError> {
    match return_type {
        FieldType::UInt => {
            if field_hash.is_empty() {
                Ok(Field::UInt(0_u64))
            }
            let mut sum = 0_u64;
            for (field, cnt) in field_hash {
                sum += field.to_uint().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))? * cnt;
            }
            Ok(Field::UInt(sum))
        }
        FieldType::Int => {
            if field_hash.is_empty() {
                Ok(Field::Int(0_i64))
            }
            let mut sum = 0_i64;
            for (field, cnt) in field_hash {
                sum += field.to_int().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))? * (cnt as i64);
            }
            Ok(Field::Int(sum))
        }
        FieldType::Float => {
            if field_hash.is_empty() {
                Ok(Field::Float(OrderedFloat::from(0_f64)))
            }
            let mut sum = 0_f64;
            for (field, cnt) in field_hash {
                sum += field.to_float().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))? * (cnt as f64);
            }
            Ok(Field::Float(OrderedFloat::from(sum)))
        }
        FieldType::Decimal => {
            if field_hash.is_empty() {
                Ok(Field::Decimal(Decimal::from(0_f64)))
            }
            let mut sum = Decimal::from_f64(0_f64)?;
            for (field, cnt) in field_hash {
                sum += field.to_decimal().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))? * Decimal::from_u64(*cnt);
            }
            Ok(Field::Decimal(sum))
        }
        _ => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Not supported return type {} for {}", return_type, Sum.to_string())))),
    }
}
