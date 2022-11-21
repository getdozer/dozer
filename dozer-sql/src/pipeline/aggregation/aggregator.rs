use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Float, Int};
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Aggregator {
    Count,
    IntegerSum,
    FloatSum,
}

const COUNT_AGGREGATOR_ID: u8 = 0x02;
const INTEGER_SUM_AGGREGATOR_ID: u8 = 0x01;
const FLOAT_SUM_AGGREGATOR_ID: u8 = 0x01;

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Aggregator {
    pub(crate) fn get_return_type(&self, from: FieldType) -> FieldType {
        match (&self, from) {
            (Aggregator::Count, _) => FieldType::Int,
            (Aggregator::IntegerSum, FieldType::Int) => FieldType::Int,
            (Aggregator::FloatSum, FieldType::Float) => FieldType::Float,
            _ => from,
        }
    }

    pub(crate) fn get_type(&self) -> u8 {
        match &self {
            Aggregator::Count => COUNT_AGGREGATOR_ID,
            Aggregator::IntegerSum => INTEGER_SUM_AGGREGATOR_ID,
            Aggregator::FloatSum => FLOAT_SUM_AGGREGATOR_ID,
        }
    }

    pub(crate) fn insert(
        &self,
        curr_state: Option<&[u8]>,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                Ok(Vec::from((prev + 1).to_ne_bytes()))
            }
            Aggregator::IntegerSum => {
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
            Aggregator::FloatSum => {
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
        }
    }

    pub(crate) fn update(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                Ok(Vec::from((prev).to_ne_bytes()))
            }
            Aggregator::IntegerSum => {
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
            Aggregator::FloatSum => {
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
        }
    }

    pub(crate) fn delete(
        &self,
        curr_state: Option<&[u8]>,
        old: &Field,
    ) -> Result<Vec<u8>, PipelineError> {
        match &self {
            Aggregator::Count => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                Ok(Vec::from((prev - 1).to_ne_bytes()))
            }
            Aggregator::IntegerSum => {
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
            Aggregator::FloatSum => {
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
        }
    }

    pub(crate) fn get_value(&self, f: &[u8]) -> Field {
        match &self {
            Aggregator::Count => Int(i64::from_ne_bytes(f.try_into().unwrap())),
            Aggregator::IntegerSum => Int(i64::from_ne_bytes(f.try_into().unwrap())),
            Aggregator::FloatSum => Float(OrderedFloat(f64::from_ne_bytes(f.try_into().unwrap()))),
        }
    }
}
