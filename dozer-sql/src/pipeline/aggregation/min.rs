use std::cmp::min;
use chrono::{DateTime, NaiveDateTime, Offset, Utc};
use num_traits::FromPrimitive;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::{Decimal, Float, Int, Timestamp};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;

pub struct MinAggregator {}

impl MinAggregator {
    const _AGGREGATOR_ID: u8 = 0x03;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Timestamp => FieldType::Timestamp,
            _ => from
        }
    }

    pub(crate) fn _get_type() -> u8 {
        MinAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(curr_state: Option<&[u8]>, new: &Field) -> Result<Vec<u8>, PipelineError> {
        match *new {
            Int(_i) => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                let curr = match &new {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MIN".to_string()));
                    }
                };

                Ok(Vec::from(min(prev, *curr).to_ne_bytes()))
            },
            Float(_f) => {
                let prev = OrderedFloat(match curr_state {
                    Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_f64,
                });

                let curr = match &new {
                    Float(i) => i,
                    _ => {
                        return Err(InvalidOperandType("MIN".to_string()));
                    }
                };

                Ok(Vec::from(min(prev, *curr).to_ne_bytes()))
            },
            _ => Err(InvalidOperandType("MIN".to_string()))
        }
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        curr_states: &Option<Vec<u8>>,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match Self::delete(curr_state, old, curr_states).unwrap().first() {
            Some(v) => i64::from(*v),
            None => 0_i64,
        };

        let curr = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("MIN".to_string()));
            }
        };

        Ok(Vec::from(min(prev, *curr).to_ne_bytes()))
    }

    pub(crate) fn delete(
        _curr_state: Option<&[u8]>,
        old: &Field,
        curr_states: &Option<Vec<u8>>,
    ) -> Result<Vec<u8>, PipelineError> {
        let mut prev = i64::MIN;

        let curr = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("MIN".to_string()));
            }
        };

        if let Some(states) = curr_states.clone() {
            for state in states {
                if i64::from(state) != *curr {
                    prev = min(prev, i64::from(state))
                }
            };
        };

        Ok(Vec::from(prev.to_ne_bytes()))
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