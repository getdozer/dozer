use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{deserialize, deserialize_decimal, deserialize_f64, deserialize_i64, deserialize_u64};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType};
use std::ops::{Add, Sub};

pub struct SumAggregator {}
const AGGREGATOR_NAME: &str = "SUM";

impl SumAggregator {

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Float => FieldType::Float,
            FieldType::Int => FieldType::Int,
            FieldType::UInt => FieldType::UInt,
            _ => from,
        }
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
                let curr = &Field::to_decimal(new).unwrap();
                let r_bytes = (prev.add(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr = &OrderedFloat(Field::to_float(new).unwrap());
                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr = &Field::to_int(new).unwrap();
                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::UInt => {
                let prev = deserialize_u64!(cur_state);
                let curr = &Field::to_uint(new).unwrap();
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
                let curr_del = &Field::to_decimal(old).unwrap();
                let curr_added = &Field::to_decimal(new).unwrap();
                let r_bytes = prev.sub(curr_del).add(curr_added).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr_del = &OrderedFloat(Field::to_float(old).unwrap());
                let curr_added = &OrderedFloat(Field::to_float(new).unwrap());
                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr_del = &Field::to_int(old).unwrap();
                let curr_added = &Field::to_int(new).unwrap();
                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::UInt => {
                let prev = deserialize_u64!(cur_state);
                let curr_del = &Field::to_uint(old).unwrap();
                let curr_added = &Field::to_uint(new).unwrap();
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
                let curr = &Field::to_decimal(old).unwrap();
                let r_bytes = (prev.sub(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat::from(deserialize_f64!(cur_state));
                let curr = &OrderedFloat(Field::to_float(old).unwrap());
                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = deserialize_i64!(cur_state);
                let curr = &Field::to_int(old).unwrap();
                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::UInt => {
                let prev = deserialize_u64!(cur_state);
                let curr = &Field::to_uint(old).unwrap();
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
            FieldType::UInt => Ok(Field::UInt(u64::from_be_bytes(deserialize!(f)))),
            _ => Err(PipelineError::DataTypeMismatch),
        }
    }
}
