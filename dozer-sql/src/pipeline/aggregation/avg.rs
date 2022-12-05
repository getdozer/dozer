use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{
    check_nan_f64, deserialize_u8, field_extract_f64, field_extract_i64, to_bytes, try_unwrap,
};

use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Float, Int};
use dozer_types::types::{Field, FieldType};
use std::string::ToString;

pub struct AvgAggregator {}
const AGGREGATOR_NAME: &str = "AVG";

impl AvgAggregator {
    const _AGGREGATOR_ID: u32 = 0x03;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
            _ => from,
        }
    }

    pub(crate) fn _get_type() -> u32 {
        AvgAggregator::_AGGREGATOR_ID
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
                let avg = try_unwrap!(Self::calc_i64_average(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
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
                let avg = (try_unwrap!(Self::calc_i64_average(ptx, aggregators_db))).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
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
                let avg = try_unwrap!(Self::calc_i64_average(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_ne_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
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
        let mut new_count = prev_count;
        if decr {
            new_count -= val_delta;
        } else {
            new_count += val_delta;
        }
        if new_count < 1 {
            try_unwrap!(ptx.del(aggregators_db, key, Option::from(to_bytes!(new_count))));
        } else {
            try_unwrap!(ptx.put(aggregators_db, key, to_bytes!(new_count)));
        }
    }

    fn calc_f64_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<f64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = 0_f64;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = f64::from_ne_bytes((cur.0).try_into().unwrap());
            let get_count = ptx.get(aggregators_db, cur.0);
            if get_count.is_ok() {
                let count = deserialize_u8!(try_unwrap!(get_count));
                total_count += count;
                total_sum += val * f64::from(count);
            }
            exist = ptx_cur.next()?;
        }
        Ok(check_nan_f64!(total_sum / f64::from(total_count)))
    }

    fn calc_i64_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<i64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = 0_i64;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = i64::from_ne_bytes((cur.0).try_into().unwrap());
            let get_count = ptx.get(aggregators_db, cur.0);
            if get_count.is_ok() {
                let count = deserialize_u8!(try_unwrap!(get_count));
                total_count += count;
                total_sum += val * i64::from(count);
            }
            exist = ptx_cur.next()?;
        }
        Ok(check_nan_f64!(total_sum as f64 / total_count as f64) as i64)
    }
}
