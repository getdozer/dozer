use std::ops::Div;
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Float, Int};
use dozer_types::types::{Field, FieldType};

pub struct AvgAggregator {}

impl AvgAggregator {
    const AGGREGATOR_ID: u32 = 0x05;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
            _ => from
        }
    }

    pub(crate) fn get_type() -> u32 {
        AvgAggregator::AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        new: &Field,
        curr_count: u64,
        _ptx: PrefixTransaction,
        _db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *new {
            Float(_f) => {
                let prev_avg = match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                };
                let prev_count = curr_count;
                let prev_sum = prev_avg * prev_count as f64;
                let new_val = match &new {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                let mut total_count = prev_count as u8;
                let mut total_sum = prev_sum;

                total_count += 1;
                total_sum += new_val.0;
                let avg = if total_sum.div(f64::from(total_count)).is_nan() { 0_f64 } else { total_sum.div(f64::from(total_count)) };
                Ok(Vec::from(avg.to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("AVG".to_string()))
        }
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_count: u64,
        _ptx: PrefixTransaction,
        _db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Float(_f) => {
                let prev_avg = match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                };
                let prev_count = curr_count;
                let prev_sum = prev_avg * prev_count as f64;
                let old_val = match &old {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                        let new_val = match &new {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                let total_count = prev_count as u8;
                let mut total_sum = prev_sum;

                total_sum -= old_val.0;
                total_sum += new_val.0;
                let avg = if total_sum.div(f64::from(total_count)).is_nan() { 0_f64 } else { total_sum.div(f64::from(total_count)) };
                Ok(Vec::from(avg.to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("AVG".to_string()))
        }
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_count: u64,
        _ptx: PrefixTransaction,
        _db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Float(_f) => {
                let prev_avg = match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                };
                let prev_count = curr_count;
                let prev_sum = prev_avg * prev_count as f64;
                let old_val = match &old {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                let mut total_count = prev_count as u8;
                let mut total_sum = prev_sum;

                total_count -= 1;
                total_sum -= old_val.0;
                let avg = if total_sum.div(f64::from(total_count)).is_nan() { 0_f64 } else { total_sum.div(f64::from(total_count)) };
                Ok(Vec::from(avg.to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("AVG".to_string()))
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Field {
        match from {
            FieldType::Int => Int(i64::from_ne_bytes(f.try_into().unwrap())),
            FieldType::Float => Float(OrderedFloat(f64::from_ne_bytes(f.try_into().unwrap()))),
            _ => Field::Null
        }
    }
}