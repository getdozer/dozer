use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType};
use dozer_types::types::Field::{Float, Int};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;

pub struct IntegerSumAggregator {}

impl IntegerSumAggregator {
    const _AGGREGATOR_ID: u8 = 0x01;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Int
    }

    pub(crate) fn _get_type() -> u8 {
        IntegerSumAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev + *curr).to_ne_bytes()))
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr_del = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };
        let curr_added = match &new {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        old: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = match curr_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let curr = match &old {
            Int(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr).to_ne_bytes()))
    }

    pub(crate) fn get_value(f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }
}

pub struct FloatSumAggregator {}

impl FloatSumAggregator {
    const _AGGREGATOR_ID: u8 = 0x01;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Float
    }

    pub(crate) fn _get_type() -> u8 {
        FloatSumAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        curr_state: Option<&[u8]>,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = OrderedFloat(match curr_state {
            Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_f64,
        });

        let curr = match &new {
            Float(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev + *curr).to_ne_bytes()))
    }

    pub(crate) fn update(
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = OrderedFloat(match curr_state {
            Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_f64,
        });

        let curr_del = match &old {
            Float(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };
        let curr_added = match &new {
            Float(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
    }

    pub(crate) fn delete(
        curr_state: Option<&[u8]>,
        old: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        let prev = OrderedFloat(match curr_state {
            Some(v) => f64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_f64,
        });

        let curr = match &old {
            Float(i) => i,
            _ => {
                return Err(InvalidOperandType("SUM".to_string()));
            }
        };

        Ok(Vec::from((prev - *curr).to_ne_bytes()))
    }

    pub(crate) fn get_value(f: &[u8]) -> Field {
        Float(OrderedFloat(f64::from_ne_bytes(f.try_into().unwrap())))
    }
}