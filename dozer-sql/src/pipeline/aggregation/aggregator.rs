use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use crate::pipeline::aggregation::avg::AvgAggregator;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Avg,
    Count,
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
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::insert(curr_state, new, curr_count),
            Aggregator::Count => CountAggregator::insert(curr_state, new),
            Aggregator::Sum => SumAggregator::insert(curr_state, new),
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::update(curr_state, old, new, curr_count),
            Aggregator::Count => CountAggregator::update(curr_state, old, new),
            Aggregator::Sum => SumAggregator::update(curr_state, old, new),
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Avg => AvgAggregator::delete(curr_state, old, curr_count),
            Aggregator::Count => CountAggregator::delete(curr_state, old),
            Aggregator::Sum => SumAggregator::delete(curr_state, old),
        }
    }

    pub(crate) fn get_value(&self, v: &[u8], from: FieldType) -> Field {
        match &self {
            Aggregator::Avg => AvgAggregator::get_value(v, from),
            Aggregator::Count => CountAggregator::get_value(v),
            Aggregator::Sum => SumAggregator::get_value(v, from),
        }
    }
}


#[macro_export]
macro_rules! deserialize_f64 {
    ($stmt:expr) => {
         match $stmt {
            Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_f64,
        }
    }
}

#[macro_export]
macro_rules! deserialize_i64 {
    ($stmt:expr) => {
         match $stmt {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        }
    }
}

#[macro_export]
macro_rules! deserialize_u8 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => u8::from_ne_bytes(v.try_into().unwrap()),
            None => 0_u8,
        }
    }
}

#[macro_export]
macro_rules! field_extract_f64 {
    ($stmt:expr, $agg:expr) => {
         match $stmt {
            Float(i) => i,
            _ => {
                return Err(InvalidOperandType($agg.to_string()));
            }
        }
    }
}

#[macro_export]
macro_rules! field_extract_i64 {
    ($stmt:expr, $agg:expr) => {
         match $stmt {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType($agg.to_string()));
            }
        }
    }
}

#[macro_export]
macro_rules! check_nan_f64 {
    ($stmt:expr) => {
         if $stmt.is_nan() {
            0_f64
        } else {
            $stmt
        }
    }
}

#[macro_export]
macro_rules! try_unwrap {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    }
}

