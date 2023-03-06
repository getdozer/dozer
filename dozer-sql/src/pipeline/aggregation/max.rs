use hashbrown::HashMap;
use dozer_core::errors::ExecutionError::InvalidOperation;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::tonic::codegen::Body;
use crate::pipeline::aggregation::aggregator::{Aggregator, update_map};
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use crate::pipeline::expression::aggregate::AggregateFunctionType::Max;

pub struct MaxAggregator {
    current_state: HashMap<Field, u64>,
}

impl MaxAggregator {
    pub fn new() -> Self {
        Self {
            current_state: HashMap::new(),
        }
    }
}

impl Aggregator for MaxAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {} for {}", old, Max.to_string()))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Max.to_string()))))
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match old.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(old, 1_u64, true, &mut self.current_state);
                    get_max(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, old, Max.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", old, Max.to_string())))),
        }
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match new.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    update_map(new, 1_u64, true, &mut self.current_state);
                    get_max(&self.current_state, return_type)
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {} for {}", field_type, new, Max.to_string()))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {} for {}", new, Max.to_string())))),
        }
    }
}

fn get_max(field_hash: &HashMap<Field, u64>, return_type: FieldType) -> Result<Field, PipelineError> {
    let val: Field = Vec::from(field_hash.keys().sorted()).get(field_hash.keys().len() - 1).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type))))?;
    match return_type {
        FieldType::UInt => Ok(Field::UInt(val.to_uint().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type))))?)),
        FieldType::Int => Ok(Field::Int(val.to_int().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type))))?)),
        FieldType::Float => Ok(Field::Float(OrderedFloat::from(val.to_float().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type))))?))),
        FieldType::Decimal => Ok(Field::Decimal(val.to_decimal().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type))))?)),
        FieldType::Timestamp => Ok(Field::Timestamp(val.to_timestamp().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type)))).unwrap()?)),
        FieldType::Date => Ok(Field::Date(val.to_date().map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to calculate max with return type {}", return_type)))).unwrap()?)),
        _ => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Not supported return type {} for {}", return_type, Max.to_string())))),
    }
}
