use std::collections::HashMap;
use std::ops::{Add, Div};
use num_traits::{FromPrimitive, Zero};
use dozer_core::storage::common::{Database, RwTransaction};
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
        mut ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *new {
            Int(_i) => {
                let new_val = match &new {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                println!("new_val {}", new_val);

                // add count + 1 to the value when inserted
                let get_prev_count = ptx.get(&db, new_val.to_ne_bytes().as_slice());
                // when the value already exist in the key, value store
                if get_prev_count.is_ok() {
                    let prev_count = match get_prev_count.unwrap() {
                        Some(v) => u8::from_ne_bytes(v.try_into().unwrap()),
                        None => 0_u8,
                    };
                    ptx.put(&db, new_val.to_ne_bytes().as_slice(), (prev_count + 1).to_ne_bytes().as_slice()).unwrap();
                }
                // when the value doesn't exist in the key, value store
                else {
                    ptx.put(&db, new_val.to_ne_bytes().as_slice(), 1_f64.to_ne_bytes().as_slice()).unwrap();
                }

                // get the prefix aware pointer cursor for lopping
                let ptx_cur = ptx.open_cursor(&db).unwrap_or_else(|e| panic!("{}", e.to_string()));
                let mut total_count = 0_u8;
                let mut total_sum = 0_i64;

                // looping over the key, value pairs
                let mut exist = ptx_cur.first()?;
                while exist {
                    let cur = ptx_cur.read().unwrap_or_else(|e| panic!("{}", e.to_string())).unwrap();
                    let val = i64::from_ne_bytes((cur.0).try_into().unwrap());
                    let get_count = ptx.get(&db, cur.0);
                    if get_count.is_ok() {
                        let count = match get_count.unwrap() {
                            Some(v) => u8::from_ne_bytes(v.try_into().unwrap()),
                            None => 0_u8,
                        };
                        total_count += count;
                        println!("adding count {}", count);
                        total_sum += val * i64::from(count);
                        println!("adding sum {}", val * i64::from(count));
                    }
                    exist = ptx_cur.next()?;
                }
                println!("total_count {}", total_count);
                println!("total_sum {}", total_sum);
                let avg = if i64::from(total_count).is_zero() { 0_i64 } else { total_sum.div(i64::from(total_count)) };
                println!("avg {}\n", avg);
                Ok(Vec::from(avg.to_ne_bytes()))
            },
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
        ptx: PrefixTransaction,
        db: &Database,
    ) -> Result<Vec<u8>, PipelineError> {
        match *old {
            Int(_i) => {
                let prev = match curr_state {
                    Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
                    None => 0_i64,
                };

                let curr_del = match &old {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };
                let curr_added = match &new {
                    Int(i) => i,
                    _ => {
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };

                Ok(Vec::from((prev - *curr_del + *curr_added).to_ne_bytes()))
            },
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
                let mut total_count = prev_count as u8;
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
        mut curr_count: u64,
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
                        return Err(InvalidOperandType("AVG".to_string()));
                    }
                };

                Ok(Vec::from((prev - *curr).to_ne_bytes()))
            },
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