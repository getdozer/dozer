use hashbrown::HashMap;
use num_traits::FromPrimitive;
use dozer_core::errors::ExecutionError::InvalidOperation;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::tonic::codegen::Body;
use crate::pipeline::aggregation::aggregator::{Aggregator, update_map};
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Avg;

pub struct AvgAggregator {
    current_state: HashMap<Field, u64>,
}

impl AvgAggregator {
    pub fn new() -> Self {
        Self {
            current_state: HashMap::new(),
        }
    }
}

impl Aggregator for AvgAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {} for {}", old, Avg.to_string()))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Avg.to_string()))))
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match old.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(old, 1_u64, true, &mut self.current_state);
                    get_average(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, old, Avg.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", old, Avg.to_string())))),
        }
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match new.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(new, 1_u64, false, &mut self.current_state);
                    get_average(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, new, Avg.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Avg.to_string())))),
        }
    }
}

fn get_average(field_hash: &HashMap<Field, u64>, return_type: FieldType) -> Result<Field, PipelineError> {
    match return_type {
        FieldType::UInt => {
            let mut sum = 0_u64;
            let mut count = 0_u64;
            for (field, cnt) in field_hash {
                sum += field.to_uint().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate average while parsing {}", field))))?;
                count += cnt;
            }
            Ok(Field::UInt(sum / count))
        }
        FieldType::Int => {
            let mut sum = 0_i64;
            let mut count = 0_i64;
            for (field, cnt) in field_hash {
                sum += field.to_int().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate average while parsing {}", field))))?;
                count += cnt as i64;
            }
            Ok(Field::Int(sum / count))
        }
        FieldType::Float => {
            let mut sum = 0_f64;
            let mut count = 0_f64;
            for (field, cnt) in field_hash {
                sum += field.to_float().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate average while parsing {}", field))))?;
                count += cnt as f64;
            }
            Ok(Field::Float(OrderedFloat::from(sum / count)))
        }
        FieldType::Decimal => {
            let mut sum = Decimal::from_f64(0_f64);
            let mut count = Decimal::from_f64(0_f64);
            for (field, cnt) in field_hash {
                sum += field.to_decimal().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate average while parsing {}", field))))?;
                count += Decimal::from_u64(*cnt);
            }
            Ok(Field::Decimal(sum / count))
        }
        _ => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Not supported return type {} for {}", return_type, Avg.to_string())))),
    }
}
