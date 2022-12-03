use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    //  Count,
    Sum,
}

pub enum AggregatorStoreType {
    ByteArray,
    Database,
}

pub enum AggregationResult {
    ByteArray(Field, Vec<u8>),
    Database(Field),
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Aggregator {
    pub(crate) fn get_store_type(&self) -> AggregatorStoreType {
        match &self {
            //   Aggregator::Count => CountAggregator::_get_type(),
            Aggregator::Sum => SumAggregator::get_store_type(),
        }
    }

    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            //   (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::Sum, from) => SumAggregator::get_return_type(from),
        }
    }

    pub(crate) fn _get_type(&self) -> u32 {
        match &self {
            //   Aggregator::Count => CountAggregator::_get_type(),
            Aggregator::Sum => SumAggregator::_get_type(),
        }
    }

    pub(crate) fn insert(
        &self,
        curr_state: Option<&[u8]>,
        new: &Field,
        return_type: FieldType,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            //  Aggregator::Count => CountAggregator::insert(curr_state, new, return_type),
            Aggregator::Sum => SumAggregator::insert(curr_state, new, return_type),
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            //  Aggregator::Count => CountAggregator::update(curr_state, old, new, return_type),
            Aggregator::Sum => SumAggregator::update(curr_state, old, new, return_type),
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        return_type: FieldType,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            //   Aggregator::Count => CountAggregator::delete(curr_state, old, return_type),
            Aggregator::Sum => SumAggregator::delete(curr_state, old, return_type),
        }
    }

    pub(crate) fn get_value(&self, v: &[u8], from: FieldType) -> Field {
        match &self {
            //   Aggregator::Count => CountAggregator::get_value(v),
            Aggregator::Sum => SumAggregator::get_value(v, from),
        }
    }
}
