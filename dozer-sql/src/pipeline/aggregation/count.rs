use dozer_core::errors::ExecutionError::InvalidOperation;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Count;

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
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {} for {}", old, Count.to_string()))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Count.to_string()))))?;
        Ok(Field::UInt(self.current_state))
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match old.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    self.current_state += 1;
                    Ok(Field::UInt(self.current_state))
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, old, Count.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {} for {}", old, Count.to_string())))),
        }
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match new.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    self.current_state += 1;
                    Ok(Field::UInt(self.current_state))
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, new, Count.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Count.to_string())))),
        }
    }
}
