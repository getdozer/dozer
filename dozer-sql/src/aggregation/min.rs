use crate::aggregation::aggregator::Aggregator;
use crate::errors::PipelineError;
use dozer_sql_expression::aggregate::AggregateFunctionType::{self, Min};

use dozer_types::types::{Field, FieldType};

use super::aggregator::OrderedAggregatorState;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct MinAggregator {
    current_state: Option<OrderedAggregatorState>,
    return_type: Option<FieldType>,
}

impl MinAggregator {
    pub fn new() -> Self {
        Self {
            current_state: None,
            return_type: None,
        }
    }
}

impl Aggregator for MinAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.current_state = OrderedAggregatorState::new(AggregateFunctionType::Min, return_type);
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        let state = self.get_state()?;
        for field in old {
            state.decr(field)?;
        }
        Ok(state.get_min())
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        let state = self.get_state()?;
        for field in new {
            state.incr(field)?;
        }
        Ok(state.get_min())
    }
}

impl MinAggregator {
    fn get_state(&mut self) -> Result<&mut OrderedAggregatorState, PipelineError> {
        self.current_state.as_mut().ok_or_else(|| {
            match self
                .return_type
                .expect("MinAggregator processor not initialized")
            {
                typ @ (FieldType::Boolean
                | FieldType::String
                | FieldType::Text
                | FieldType::Binary
                | FieldType::Json
                | FieldType::Point) => PipelineError::InvalidReturnType(format!(
                    "Not supported return type {typ} for {Min}"
                )),
                _ => panic!("MinAggregator processor not correctly initialized"),
            }
        })
    }
}
