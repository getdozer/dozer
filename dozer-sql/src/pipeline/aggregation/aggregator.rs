use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::sum::{FloatSumAggregator, IntegerSumAggregator};
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use crate::pipeline::aggregation::avg::AvgAggregator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Avg,
    Count,
    IntegerSum,
    FloatSum,
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Aggregator {
    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            (Aggregator::Avg, from) => AvgAggregator::get_return_type(from),
            (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::IntegerSum, FieldType::Int) => IntegerSumAggregator::get_return_type(),
            (Aggregator::FloatSum, FieldType::Float) => FloatSumAggregator::get_return_type(),
            _ => from,
        }
    }

    pub(crate) fn get_type(&self) -> u32 {
        match &self {
            Aggregator::Avg => AvgAggregator::get_type(),
            Aggregator::Count => CountAggregator::get_type(),
            Aggregator::IntegerSum => IntegerSumAggregator::_get_type(),
            Aggregator::FloatSum => FloatSumAggregator::_get_type(),
        }
    }

    pub(crate) fn insert(
        &self,
        curr_state: Option<&[u8]>,
        new: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::insert(curr_state, new, curr_count, ptx, db),
            Aggregator::Count => CountAggregator::insert(curr_state, new),
            Aggregator::IntegerSum => IntegerSumAggregator::insert(curr_state, new),
            Aggregator::FloatSum => FloatSumAggregator::insert(curr_state, new),
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::update(curr_state, old, new, curr_count, ptx, db),
            Aggregator::Count => CountAggregator::update(curr_state, old, new),
            Aggregator::IntegerSum => IntegerSumAggregator::update(curr_state, old, new),
            Aggregator::FloatSum => FloatSumAggregator::update(curr_state, old, new),
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::delete(curr_state, old, curr_count, ptx, db),
            Aggregator::Count => CountAggregator::delete(curr_state, old),
            Aggregator::IntegerSum => IntegerSumAggregator::delete(curr_state, old),
            Aggregator::FloatSum => FloatSumAggregator::delete(curr_state, old),
        }
    }

    pub(crate) fn get_value(&self, v: &[u8], from: FieldType) -> Field {
        match &self {
            Aggregator::Avg => AvgAggregator::get_value(v, from),
            Aggregator::Count => CountAggregator::get_value(v),
            Aggregator::IntegerSum => IntegerSumAggregator::get_value(v),
            Aggregator::FloatSum => FloatSumAggregator::get_value(v),
        }
    }
}
