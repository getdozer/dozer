use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Float, Int};
use dozer_types::types::{Field, FieldType};
use std::ops::Div;
use std::string::ToString;
use crate::{check_nan_f64, deserialize_f64, field_extract_f64};

pub struct AvgAggregator {}
const AGGREGATOR_NAME: &str = "AVG";

impl AvgAggregator {
    const _AGGREGATOR_ID: u32 = 0x05;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Decimal,
            FieldType::Float => FieldType::Decimal,
            _ => from,
        }
    }

    pub(crate) fn _get_type() -> u32 {
        AvgAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        new: &Field,
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match *new {
            Float(_f) => {
                println!("[ FLOAT AVG ]");
                let prev_avg = deserialize_f64!(curr_state);
                let prev_sum = prev_avg * curr_count as f64;
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                let mut total_count = curr_count as u8;
                let mut total_sum = prev_sum;

                total_count += 1;
                total_sum += new_val.0;
                let avg = check_nan_f64!(total_sum.div(f64::from(total_count)));
                Ok(Vec::from(avg.to_ne_bytes()))
            }
            _ => Err(InvalidOperandType("AVG".to_string())),
        }
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Float(_f) => {
                let prev_avg = deserialize_f64!(curr_state);
                let prev_sum = prev_avg * curr_count as f64;
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                let total_count = curr_count as u8;
                let mut total_sum = prev_sum;

                total_sum -= old_val.0;
                total_sum += new_val.0;
                let avg = check_nan_f64!(total_sum.div(f64::from(total_count)));
                Ok(Vec::from(avg.to_ne_bytes()))
            }
            _ => Err(InvalidOperandType("AVG".to_string())),
        }
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_count: u64,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Float(_f) => {
                let prev_avg = deserialize_f64!(curr_state);
                let prev_count = curr_count;
                let prev_sum = prev_avg * prev_count as f64;
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                let mut total_count = prev_count as u8;
                let mut total_sum = prev_sum;

                total_count -= 1;
                total_sum -= old_val.0;
                let avg = check_nan_f64!(total_sum.div(f64::from(total_count)));
                Ok(Vec::from(avg.to_ne_bytes()))
            }
            _ => Err(InvalidOperandType("AVG".to_string())),
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Field {
        match from {
            FieldType::Int => Int(i64::from_ne_bytes(f.try_into().unwrap())),
            FieldType::Float => Float(OrderedFloat(f64::from_ne_bytes(f.try_into().unwrap()))),
            _ => Field::Null,
        }
    }
}
