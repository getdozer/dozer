use crate::pipeline::aggregation::avg::AvgAggregator;
use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::max::MaxAggregator;
use crate::pipeline::aggregation::min::MinAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;

use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Avg,
    Count,
    Max,
    Min,
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
        write!(f, "{self:?}")
    }
}

impl Aggregator {
    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            (Aggregator::Avg, _) => AvgAggregator::get_return_type(from),
            (Aggregator::Count, _) => CountAggregator::get_return_type(),
            (Aggregator::Max, from) => MaxAggregator::get_return_type(from),
            (Aggregator::Min, from) => MinAggregator::get_return_type(from),
            (Aggregator::Sum, from) => SumAggregator::get_return_type(from),
        }
    }

    pub(crate) fn insert(
        &self,
        cur_state: Option<&[u8]>,
        new: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
        agg_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::insert(cur_state, new, return_type, txn, agg_db),
            Aggregator::Count => CountAggregator::insert(cur_state, new, return_type, txn),
            Aggregator::Max => MaxAggregator::insert(cur_state, new, return_type, txn, agg_db),
            Aggregator::Min => MinAggregator::insert(cur_state, new, return_type, txn, agg_db),
            Aggregator::Sum => SumAggregator::insert(cur_state, new, return_type, txn),
        }
    }

    pub(crate) fn update(
        &self,
        cur_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
        agg_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::update(cur_state, old, new, return_type, txn, agg_db),
            Aggregator::Count => CountAggregator::update(cur_state, old, new, return_type, txn),
            Aggregator::Max => MaxAggregator::update(cur_state, old, new, return_type, txn, agg_db),
            Aggregator::Min => MinAggregator::update(cur_state, old, new, return_type, txn, agg_db),
            Aggregator::Sum => SumAggregator::update(cur_state, old, new, return_type, txn),
        }
    }

    pub(crate) fn delete(
        &self,
        cur_state: Option<&[u8]>,
        old: &Field,
        return_type: FieldType,
        txn: &mut PrefixTransaction,
        agg_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::delete(cur_state, old, return_type, txn, agg_db),
            Aggregator::Count => CountAggregator::delete(cur_state, old, return_type, txn),
            Aggregator::Max => MaxAggregator::delete(cur_state, old, return_type, txn, agg_db),
            Aggregator::Min => MinAggregator::delete(cur_state, old, return_type, txn, agg_db),
            Aggregator::Sum => SumAggregator::delete(cur_state, old, return_type, txn),
        }
    }
}

#[macro_export]
macro_rules! deserialize {
    ($stmt:expr) => {
        $stmt.try_into().unwrap()
    };
}

#[macro_export]
macro_rules! deserialize_f64 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => f64::from_be_bytes(deserialize!(v)),
            None => 0_f64,
        }
    };
}

#[macro_export]
macro_rules! deserialize_i64 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => i64::from_be_bytes(deserialize!(v)),
            None => 0_i64,
        }
    };
}

#[macro_export]
macro_rules! deserialize_u64 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => u64::from_be_bytes(deserialize!(v)),
            None => 0_u64,
        }
    };
}

#[macro_export]
macro_rules! deserialize_decimal {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => Decimal::deserialize(deserialize!(v)),
            None => Decimal::from(0),
        }
    };
}

#[macro_export]
macro_rules! deserialize_u8 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => u8::from_be_bytes(deserialize!(v)),
            None => 0_u8,
        }
    };
}

#[macro_export]
macro_rules! check_nan_f64 {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            0_f64
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! check_nan_decimal {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            dozer_types::rust_decimal::Decimal::zero()
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! try_unwrap {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[macro_export]
macro_rules! to_bytes {
    ($stmt:expr) => {
        $stmt.to_be_bytes().as_slice()
    };
}
