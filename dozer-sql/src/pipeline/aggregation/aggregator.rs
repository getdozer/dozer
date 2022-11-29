use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use crate::pipeline::aggregation::avg::AvgAggregator;
use crate::pipeline::aggregation::max::MaxAggregator;
use crate::pipeline::aggregation::min::MinAggregator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Avg,
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
            (Aggregator::Avg, from) => AvgAggregator::get_return_type(from),
            (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::Min, from) => MinAggregator::get_return_type(from),
            (Aggregator::Max, from) => MaxAggregator::get_return_type(from),
            (Aggregator::Sum, from) => SumAggregator::get_return_type(from),
        }
    }

    pub(crate) fn get_type(&self) -> u32 {
        match &self {
            Aggregator::Avg => AvgAggregator::get_type(),
            Aggregator::Count => CountAggregator::get_type(),
            Aggregator::Min => MinAggregator::get_type(),
            Aggregator::Max => MaxAggregator::get_type(),
            Aggregator::Sum => SumAggregator::get_type(),
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
            Aggregator::Count => CountAggregator::insert(curr_state),
            Aggregator::Min => MinAggregator::insert(curr_state, new, curr_count, ptx, db),
            Aggregator::Max => MaxAggregator::insert(curr_state, new, curr_count, ptx, db),
            Aggregator::Sum => SumAggregator::insert(curr_state, new),
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
            Aggregator::Count => CountAggregator::update(curr_state),
            Aggregator::Min => MinAggregator::update(curr_state, old, new, curr_count, ptx, db),
            Aggregator::Max => MaxAggregator::update(curr_state, old, new, curr_count, ptx, db),
            Aggregator::Sum => SumAggregator::update(curr_state, old, new),
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
            Aggregator::Count => CountAggregator::delete(curr_state),
            Aggregator::Min => MinAggregator::delete(curr_state, old, curr_count, ptx, db),
            Aggregator::Max => MaxAggregator::delete(curr_state, old, curr_count, ptx, db),
            Aggregator::Sum => SumAggregator::delete(curr_state, old),
        }
    }

    pub(crate) fn get_value(&self, v: &[u8], from: FieldType) -> Field {
        match &self {
            Aggregator::Avg => AvgAggregator::get_value(v, from),
            Aggregator::Count => CountAggregator::get_value(v),
            Aggregator::Min => MinAggregator::get_value(v, from),
            Aggregator::Max => MaxAggregator::get_value(v, from),
            Aggregator::Sum => SumAggregator::get_value(v, from),
        }
    }
}
