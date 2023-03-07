use dozer_core::errors::ExecutionError::InvalidOperation;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Count;

#[derive(Debug)]
pub struct CountAggregator {
    pub current_state: u64,
}

impl CountAggregator {
    pub fn new() -> Self {
        Self {
            current_state: 0_u64,
        }
    }
}

impl Aggregator for CountAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to update while deleting record: {} for {}", old, Count.to_string()))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to update while inserting record: {} for {}", new, Count.to_string()))))
    }

    fn delete(&mut self, _old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        self.current_state -= 1;
        get_count(self.current_state, return_type)
    }

    fn insert(&mut self, _new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        self.current_state += 1;
        get_count(self.current_state, return_type)
    }
}

fn get_count(count: u64, return_type: FieldType) -> Result<Field, PipelineError> {
    match return_type {
        FieldType::UInt => {
            Ok(Field::UInt(count))
        }
        FieldType::Int => {
            Ok(Field::Int(count as i64))
        }
        FieldType::Float => {
            Ok(Field::Float(OrderedFloat::from(count as f64)))
        }
        FieldType::Decimal => {
            Ok(Field::Decimal(Decimal::from(count as f64)))
        }
        _ => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Not supported return type {} for {}", return_type, Count.to_string())))),
    }
}
