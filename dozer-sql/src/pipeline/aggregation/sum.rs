use dozer_core::errors::ExecutionError::InvalidOperation;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::tonic::codegen::Body;
use crate::pipeline::aggregation::aggregator::{Aggregator, update_map};
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Sum;

#[derive(Debug)]
pub struct SumAggregator {
    current_state: SumAggregatorState
}

struct SumAggregatorState {
    int_state: i64,
    uint_state: u64,
    float_state: f64,
    decimal_state: Decimal,
}

impl SumAggregator {
    pub fn new() -> Self {
        Self {
            current_state: SumAggregatorState{
                int_state: 0_i64,
                uint_state: 0_u64,
                float_state: 0_f64,
                decimal_state: Decimal::from(0_f64),
            }
        }
    }
}

impl Aggregator for SumAggregator {
    fn init(&mut self, return_type: FieldType) {
        todo!()
    }

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
                    get_sum(old, &mut self.current_state, return_type, true)
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
                    get_sum(new, &mut self.current_state, return_type, false)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, new, Sum.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Sum.to_string())))),
        }
    }
}

fn get_sum(field: &Field, current_state: &mut SumAggregatorState, return_type: FieldType, decr: bool) -> Result<Field, PipelineError> {
    match return_type {
        FieldType::UInt => {
            if decr {
                current_state.uint_state -= field.to_uint().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            else {
                current_state.uint_state += field.to_uint().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            Ok(Field::UInt(current_state.uint_state))
        }
        FieldType::Int => {
            if decr {
                current_state.int_state -= field.to_int().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            else {
                current_state.int_state += field.to_int().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            Ok(Field::Int(current_state.int_state))
        }
        FieldType::Float => {
            if decr {
                current_state.float_state -= field.to_float().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            else {
                current_state.float_state += field.to_float().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            Ok(Field::Float(OrderedFloat::from(current_state.float_state)))
        }
        FieldType::Decimal => {
            if decr {
                current_state.decimal_state -= field.to_decimal().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            else {
                current_state.decimal_state += field.to_decimal().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate sum while parsing {}", field))))?;
            }
            Ok(Field::Decimal(current_state.decimal_state))
        }
        _ => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Not supported return type {} for {}", return_type, Sum.to_string())))),
    }
}
