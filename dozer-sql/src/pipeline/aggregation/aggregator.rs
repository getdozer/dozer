use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use crate::pipeline::aggregation::max::MaxAggregator;
use crate::pipeline::aggregation::min::MinAggregator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Count,
    Min,
    Max,
    Sum,
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Aggregator {
    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::Min, from) => MinAggregator::get_return_type(from),
            (Aggregator::Max, from) => MaxAggregator::get_return_type(from),
            (Aggregator::Sum, from) => SumAggregator::get_return_type(from),
        }
    }

    pub(crate) fn _get_type(&self) -> u8 {
        match &self {
            Aggregator::Count => CountAggregator::_get_type(),
            Aggregator::Min => MinAggregator::_get_type(),
            Aggregator::Max => MaxAggregator::_get_type(),
            Aggregator::Sum => SumAggregator::_get_type(),
        }
    }

    pub(crate) fn insert(
        &self,
        curr_state: Option<&[u8]>,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => CountAggregator::insert(curr_state),
            Aggregator::Min => MinAggregator::insert(curr_state, new),
            Aggregator::Max => MaxAggregator::insert(curr_state, new),
            Aggregator::Sum => SumAggregator::insert(curr_state, new),
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_states: &Option<Vec<u8>>,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => CountAggregator::update(curr_state),
            Aggregator::Min => MinAggregator::update(curr_state, old, new, curr_states),
            Aggregator::Max => MaxAggregator::update(curr_state, old, new),
            Aggregator::Sum => SumAggregator::update(curr_state, old, new),
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_states: &Option<Vec<u8>>,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => CountAggregator::delete(curr_state),
            Aggregator::Min => MinAggregator::delete(curr_state, old, curr_states),
            Aggregator::Max => MaxAggregator::delete(curr_state, old),
            Aggregator::Sum => SumAggregator::delete(curr_state, old),
        }
    }

    pub(crate) fn get_value(&self, v: &[u8], from: FieldType) -> Field {
        match &self {
            Aggregator::Count => CountAggregator::get_value(v),
            Aggregator::Min => MinAggregator::get_value(v, from),
            Aggregator::Max => MaxAggregator::get_value(v, from),
            Aggregator::Sum => SumAggregator::get_value(v, from),
        }
    }
}
