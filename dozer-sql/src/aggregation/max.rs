use crate::aggregation::aggregator::Aggregator;
use crate::errors::PipelineError;
use dozer_sql_expression::aggregate::AggregateFunctionType::Max;
use dozer_types::types::{Field, FieldType};

use super::aggregator::OrderedAggregatorState;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct MaxAggregator {
    current_state: Option<OrderedAggregatorState>,
    return_type: Option<FieldType>,
}

impl MaxAggregator {
    pub fn new() -> Self {
        Self {
            current_state: None,
            return_type: None,
        }
    }
}

impl Aggregator for MaxAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
        self.current_state = OrderedAggregatorState::new(Max, return_type);
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
        Ok(state.get_max())
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        let state = self.get_state()?;
        for field in new {
            state.incr(field)?;
        }
        Ok(state.get_max())
    }
}

impl MaxAggregator {
    fn get_state(&mut self) -> Result<&mut OrderedAggregatorState, PipelineError> {
        self.current_state.as_mut().ok_or_else(|| {
            match self
                .return_type
                .expect("MaxAggregator processor not initialized")
            {
                typ @ (FieldType::Boolean
                | FieldType::String
                | FieldType::Text
                | FieldType::Binary
                | FieldType::Json
                | FieldType::Point) => PipelineError::InvalidReturnType(format!(
                    "Not supported return type {typ} for {Max}"
                )),
                _ => panic!("MaxAggregator processor not correctly initialized"),
            }
        })
    }
}
