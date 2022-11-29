use std::cmp::max;
use chrono::{DateTime, NaiveDateTime, Offset, Utc};
use num_traits::FromPrimitive;
use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::{Decimal, Float, Int, Timestamp};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;

pub struct MaxAggregator {}

impl MaxAggregator {
    const AGGREGATOR_ID: u32 = 0x04;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Timestamp => FieldType::Timestamp,
            _ => from
        }
    }

    pub(crate) fn get_type() -> u32 {
        MaxAggregator::AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        new: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *new {
            Int(_i) => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                let curr = match &new {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MAX".to_string()));
                    }
                };

                Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
            },
            Float(_f) => {
                let prev = OrderedFloat(match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                });

                let curr = match &new {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MAX".to_string()));
                    }
                };

                Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("MAX".to_string()))
        }
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        _new: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("MAX".to_string()));
            }
        };

        Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        old: &Field,
        curr_count: u64,
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Int(_i) => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                let curr = match &old {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MAX".to_string()));
                    }
                };

                Ok(Vec::from(max(prev, *curr).to_ne_bytes()))

                // if prev > *curr {
                //     Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
                // }
                // else {
                //
                // }

            },
            Float(_f) => {
                let prev = OrderedFloat(match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                });

                let curr = match &old {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MAX".to_string()));
                    }
                };

                Ok(Vec::from(max(prev, *curr).to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("MAX".to_string()))
        }
    }

    pub(crate) fn get_value(v: &[u8], from: FieldType) -> Field {
        match from {
            FieldType::Int => Int(i64::from_ne_bytes(v.try_into().unwrap())),
            FieldType::Float => Float(OrderedFloat(f64::from_ne_bytes(v.try_into().unwrap()))),
            FieldType::Decimal => Decimal(rust_decimal::Decimal::from_f64(
                String::from_utf8(v.to_vec())
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
                )
                .unwrap()),
            FieldType::Timestamp => {
                let date = NaiveDateTime::parse_from_str(
                    String::from_utf8(v.to_vec()).unwrap().as_str(),
                    "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap();
                Timestamp(DateTime::from_utc(date, Utc.fix()))
            },
            _ => Field::Null
        }
    }
}