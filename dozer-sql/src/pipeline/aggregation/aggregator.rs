use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;

use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use crate::pipeline::aggregation::avg::AvgAggregator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Avg,
    Count,
    Sum,
}

pub(crate) struct AggregationResult {
    pub value: Field,
    pub state: Option<Vec<u8>>,
}

impl AggregationResult {
    pub fn new(value: Field, state: Option<Vec<u8>>) -> Self {
        Self { value, state }
    }
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Aggregator {
    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            (Aggregator::Avg, _) => AvgAggregator::get_return_type(from),
            (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::Sum, from) => SumAggregator::get_return_type(from),
        }
    }

    pub(crate) fn _get_type(&self) -> u32 {
        match &self {
            Aggregator::Avg => AvgAggregator::_get_type(),
            Aggregator::Count => CountAggregator::_get_type(),
            Aggregator::Sum => SumAggregator::_get_type(),
        }
    }

    pub(crate) fn insert(
        &self,
        curr_state: Option<&[u8]>,
        new: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::insert(curr_state, new, return_type, txn),
            Aggregator::Count => CountAggregator::insert(curr_state, new, return_type, txn),
            Aggregator::Sum => SumAggregator::insert(curr_state, new, return_type, txn),
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::update(curr_state, old, new, return_type, txn),
            Aggregator::Count => CountAggregator::update(curr_state, old, new, return_type, txn),
            Aggregator::Sum => SumAggregator::update(curr_state, old, new, return_type, txn),
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::delete(curr_state, old, return_type, txn),
            Aggregator::Count => CountAggregator::delete(curr_state, old, return_type, txn),
            Aggregator::Sum => SumAggregator::delete(curr_state, old, return_type, txn),
        }
    }
}
