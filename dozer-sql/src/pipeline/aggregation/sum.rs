use crate::deserialize;
use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use std::ops::{Add, Sub};

use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType};

pub struct SumAggregator {}

impl SumAggregator {
    const _AGGREGATOR_ID: u32 = 0x01;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
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
                let prev = match cur_state {
                    Some(v) => Decimal::deserialize(deserialize!(v)),
                    None => Decimal::from(0),
                };

                let curr = match &new {
                    Field::Decimal(i) => *i,
                    Field::Null => Decimal::from(0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev.add(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = match cur_state {
                    Some(v) => i64::from_be_bytes(deserialize!(v)),
                    None => 0_i64,
                };

                let curr = match &new {
                    Field::Int(i) => i,
                    Field::Null => &0_i64,
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat(match cur_state {
                    Some(v) => f64::from_be_bytes(deserialize!(v)),
                    None => 0_f64,
                });

                let curr = match &new {
                    Field::Float(i) => i,
                    Field::Null => &OrderedFloat(0.0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev + *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType("SUM".to_string())),
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
                let prev = match cur_state {
                    Some(v) => Decimal::deserialize(deserialize!(v)),
                    None => Decimal::from(0),
                };

                let curr_del = match old {
                    Field::Decimal(i) => *i,
                    Field::Null => Decimal::from(0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };
                let curr_added = match new {
                    Field::Decimal(i) => *i,
                    Field::Null => Decimal::from(0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = prev.sub(curr_del).add(curr_added).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = match cur_state {
                    Some(v) => i64::from_be_bytes(deserialize!(v)),
                    None => 0_i64,
                };

                let curr_del = match &old {
                    Field::Int(i) => i,
                    Field::Null => &0_i64,
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };
                let curr_added = match &new {
                    Field::Int(i) => i,
                    Field::Null => &0_i64,
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat(match cur_state {
                    Some(v) => f64::from_be_bytes(deserialize!(v)),
                    None => 0_f64,
                });

                let curr_del = match &old {
                    Field::Float(i) => i,
                    Field::Null => &OrderedFloat(0.0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };
                let curr_added = match &new {
                    Field::Float(i) => i,
                    Field::Null => &OrderedFloat(0.0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev - *curr_del + *curr_added).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType("SUM".to_string())),
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
                let prev = match cur_state {
                    Some(v) => Decimal::deserialize(deserialize!(v)),
                    None => Decimal::from(0),
                };

                let curr = match old {
                    Field::Decimal(i) => *i,
                    Field::Null => Decimal::from(0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev.sub(curr)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Int => {
                let prev = match cur_state {
                    Some(v) => i64::from_be_bytes(deserialize!(v)),
                    None => 0_i64,
                };

                let curr = match &old {
                    Field::Int(i) => i,
                    Field::Null => &0_i64,
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            FieldType::Float => {
                let prev = OrderedFloat(match cur_state {
                    Some(v) => f64::from_be_bytes(deserialize!(v)),
                    None => 0_f64,
                });

                let curr = match &old {
                    Field::Float(i) => i,
                    Field::Null => &OrderedFloat(0.0),
                    _ => {
                        return Err(InvalidOperandType("SUM".to_string()));
                    }
                };

                let r_bytes = (prev - *curr).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&r_bytes, return_type)?,
                    Some(Vec::from(r_bytes)),
                ))
            }
            _ => Err(InvalidOperandType("SUM".to_string())),
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Result<Field, PipelineError> {
        match from {
            FieldType::Int => Ok(Field::Int(i64::from_be_bytes(deserialize!(f)))),
            FieldType::Float => Ok(Field::Float(OrderedFloat(f64::from_be_bytes(
                deserialize!(f),
            )))),
            FieldType::Decimal => Ok(Field::Decimal(Decimal::deserialize(deserialize!(f)))),
            _ => Err(PipelineError::DataTypeMismatch),
        }
    }
}
