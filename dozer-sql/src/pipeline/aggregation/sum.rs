use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{
    deserialize, deserialize_decimal, deserialize_f64, deserialize_i64, field_extract_decimal,
    field_extract_f64, field_extract_i64,
};
use std::ops::{Add, Sub};

use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType};

pub struct SumAggregator {}
const AGGREGATOR_NAME: &str = "SUM";

impl SumAggregator {
    const _AGGREGATOR_ID: u32 = 0x01;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Float => FieldType::Float,
            FieldType::Int => FieldType::Int,
            _ => from,
        }
    }

    pub(crate) fn _get_type() -> u32 {
        SumAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        cur_state: Option<&[u8]>,
        new: &Field,
        return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match return_type {
            FieldType::Decimal => {
                let prev = deserialize_decimal!(cur_state);
                let curr = field_extract_decimal!(&new, AGGREGATOR_NAME);
                let r_bytes = (prev.add(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr = field_extract_f64!(&new, AGGREGATOR_NAME);
                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr = field_extract_i64!(&new, AGGREGATOR_NAME);
                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn update(
        cur_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match return_type {
            FieldType::Decimal => {
                let prev = deserialize_decimal!(cur_state);
                let curr_del = field_extract_decimal!(&old, AGGREGATOR_NAME);
                let curr_added = field_extract_decimal!(&new, AGGREGATOR_NAME);
                let r_bytes = prev.sub(curr_del).add(curr_added).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr_del = field_extract_f64!(&old, AGGREGATOR_NAME);
                let curr_added = field_extract_f64!(&new, AGGREGATOR_NAME);
                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr_del = field_extract_i64!(&old, AGGREGATOR_NAME);
                let curr_added = field_extract_i64!(&new, AGGREGATOR_NAME);
                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn delete(
        cur_state: Option<&[u8]>,
        old: &Field,
        return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        match return_type {
            FieldType::Decimal => {
                let prev = deserialize_decimal!(cur_state);
                let curr = field_extract_decimal!(&old, AGGREGATOR_NAME);
                let r_bytes = (prev.sub(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr = field_extract_f64!(&old, AGGREGATOR_NAME);
                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr = field_extract_i64!(&old, AGGREGATOR_NAME);
                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Result<Field, PipelineError> {
        match from {
            FieldType::Decimal => Ok(Field::Decimal(Decimal::deserialize(deserialize!(f)))),
            FieldType::Float => Ok(Field::Float(OrderedFloat(f64::from_be_bytes(
                deserialize!(f),
            )))),
            FieldType::Int => Ok(Field::Int(i64::from_be_bytes(deserialize!(f)))),
            _ => Err(PipelineError::DataTypeMismatch),
        }
    }
}
