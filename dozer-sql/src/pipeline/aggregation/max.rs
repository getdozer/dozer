use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{deserialize_u8, field_extract_f64, field_extract_i64, to_bytes, try_unwrap};

use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Float, Int};
use dozer_types::types::{Field, FieldType};

use std::cmp::max;
use std::string::ToString;

pub struct MaxAggregator {}
const AGGREGATOR_NAME: &str = "MAX";

impl MaxAggregator {
    const _AGGREGATOR_ID: u32 = 0x03;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Decimal,
            FieldType::Float => FieldType::Decimal,
            _ => from,
        }
    }

    pub(crate) fn _get_type() -> u32 {
        MaxAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        _cur_state: Option<&[u8]>,
        new: &Field,
        return_type: FieldType,
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<AggregationResult, PipelineError> {
        match *new {
            Int(_i) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_i64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let maximum = try_unwrap!(Self::calc_i64_max(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                println!("\nnew_val {}", new_val);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let maximum = try_unwrap!(Self::calc_f64_max(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn update(
        _cur_state: Option<&[u8]>,
        old: &Field,
        new: &Field,
        return_type: FieldType,
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<AggregationResult, PipelineError> {
        match *old {
            Int(_i) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_i64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_i64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let maximum = (try_unwrap!(Self::calc_i64_max(ptx, aggregators_db))).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                println!("\nnew_val {}", new_val);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                println!("\nold_val {}", old_val);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let maximum = try_unwrap!(Self::calc_f64_max(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn delete(
        _cur_state: Option<&[u8]>,
        old: &Field,
        return_type: FieldType,
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<AggregationResult, PipelineError> {
        match *old {
            Int(_i) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_i64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let maximum = try_unwrap!(Self::calc_i64_max(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                println!("\nold_val {}", old_val);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let maximum = try_unwrap!(Self::calc_f64_max(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&maximum, return_type),
                    Some(Vec::from(maximum)),
                ))
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Field {
        match from {
            FieldType::Int => Int(i64::from_ne_bytes(f.try_into().unwrap())),
            FieldType::Float => Float(OrderedFloat(f64::from_ne_bytes(f.try_into().unwrap()))),
            _ => Field::Null,
        }
    }

    fn update_aggregator_db(
        key: &[u8],
        val_delta: u8,
        decr: bool,
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) {
        let get_prev_count = try_unwrap!(ptx.get(aggregators_db, key));
        let prev_count = deserialize_u8!(get_prev_count);
        try_unwrap!(ptx.put(
            aggregators_db,
            key,
            to_bytes!(if decr {
                prev_count - val_delta
            } else {
                prev_count + val_delta
            })
        ));
    }

    fn calc_f64_max(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<f64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut maximum = f64::MIN;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = f64::from_ne_bytes((cur.0).try_into().unwrap());
            if maximum < val {
                maximum = val
            }
            exist = ptx_cur.next()?;
        }
        Ok(maximum)
    }

    fn calc_i64_max(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<i64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut maximum = i64::MIN;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = i64::from_ne_bytes((cur.0).try_into().unwrap());
            maximum = max(maximum, val);
            exist = ptx_cur.next()?;
        }
        Ok(maximum)
    }
}
