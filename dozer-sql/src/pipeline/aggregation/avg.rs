use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{
    check_nan_f64, deserialize_u8, field_extract_decimal, field_extract_f64, field_extract_i64,
    to_bytes, try_unwrap,
};
use crate::{deserialize, field_extract_u64};
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Decimal, Float, Int, UInt};
use dozer_types::types::{Field, FieldType};
use num_traits::Zero;
use std::ops::Div;
use std::string::ToString;

pub struct AvgAggregator {}
const AGGREGATOR_NAME: &str = "AVG";

impl AvgAggregator {
    const _AGGREGATOR_ID: u32 = 0x03;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Float => FieldType::Float,
            FieldType::Int => FieldType::Int,
            FieldType::UInt => FieldType::UInt,
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
        aggregators_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match (return_type, new) {
            (FieldType::Decimal, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_decimal!(&new, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_decimal_average(ptx, aggregators_db)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(avg.as_slice(), return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Float, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Int, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_i64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_i64_average(ptx, aggregators_db)).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::UInt, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_u64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_u64_average(ptx, aggregators_db)).to_be_bytes();
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
        aggregators_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match (return_type, new) {
            (FieldType::Decimal, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_decimal!(&new, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);

                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_decimal!(&old, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_decimal_average(ptx, aggregators_db)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(avg.as_slice(), return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Float, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Int, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_i64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_i64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = (try_unwrap!(Self::calc_i64_average(ptx, aggregators_db))).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::UInt, _) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_u64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_u64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = (try_unwrap!(Self::calc_u64_average(ptx, aggregators_db))).to_be_bytes();
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
        aggregators_db: Database,
    ) -> Result<AggregationResult, PipelineError> {
        match (return_type, old) {
            (FieldType::Decimal, _) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_decimal!(&old, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_decimal_average(ptx, aggregators_db)).serialize();
                Ok(AggregationResult::new(
                    Self::get_value(avg.as_slice(), return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Float, _) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_f64_average(ptx, aggregators_db)).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::Int, _) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_i64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_i64_average(ptx, aggregators_db)).to_be_bytes();
                Ok(AggregationResult::new(
                    Self::get_value(&avg, return_type),
                    Some(Vec::from(avg)),
                ))
            }
            (FieldType::UInt, _) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_u64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate average
                let avg = try_unwrap!(Self::calc_u64_average(ptx, aggregators_db)).to_be_bytes();
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
            FieldType::Decimal => Decimal(dozer_types::rust_decimal::Decimal::deserialize(
                deserialize!(f),
            )),
            FieldType::Float => Float(OrderedFloat(f64::from_be_bytes(deserialize!(f)))),
            FieldType::Int => Int(i64::from_be_bytes(deserialize!(f))),
            FieldType::UInt => UInt(u64::from_be_bytes(deserialize!(f))),
            _ => Field::Null,
        }
    }

    fn update_aggregator_db(
        key: &[u8],
        val_delta: u8,
        decr: bool,
        ptx: &mut PrefixTransaction,
        aggregators_db: Database,
    ) {
        let get_prev_count = try_unwrap!(ptx.get(aggregators_db, key));
        let prev_count = deserialize_u8!(get_prev_count);
        let mut new_count = prev_count;
        if decr {
            new_count = new_count.wrapping_sub(val_delta);
        } else {
            new_count = new_count.wrapping_add(val_delta);
        }
        if new_count < 1 {
            try_unwrap!(ptx.del(aggregators_db, key, Option::from(to_bytes!(prev_count))));
        } else {
            try_unwrap!(ptx.put(aggregators_db, key, to_bytes!(new_count)));
        }
    }

    fn calc_f64_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: Database,
    ) -> Result<f64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = 0_f64;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = f64::from_be_bytes(deserialize!(cur.0));
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

    fn calc_decimal_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: Database,
    ) -> Result<dozer_types::rust_decimal::Decimal, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = dozer_types::rust_decimal::Decimal::zero();
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = dozer_types::rust_decimal::Decimal::deserialize(deserialize!(cur.0));
            let get_count = ptx.get(aggregators_db, cur.0);
            if get_count.is_ok() {
                let count = deserialize_u8!(try_unwrap!(get_count));
                total_count += count;
                total_sum += val * dozer_types::rust_decimal::Decimal::from(count);
            }
            exist = ptx_cur.next()?;
        }
        if total_count.is_zero() {
            Ok(dozer_types::rust_decimal::Decimal::zero())
        } else {
            Ok(total_sum.div(dozer_types::rust_decimal::Decimal::from(total_count)))
        }
    }

    fn calc_i64_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: Database,
    ) -> Result<i64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = 0_i64;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = i64::from_be_bytes(deserialize!(cur.0));
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

    fn calc_u64_average(
        ptx: &mut PrefixTransaction,
        aggregators_db: Database,
    ) -> Result<u64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut total_count = 0_u8;
        let mut total_sum = 0_u64;
        let mut exist = ptx_cur.first()?;

        // Loop through aggregators_db to calculate average
        while exist {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            let val = u64::from_be_bytes(deserialize!(cur.0));
            let get_count = ptx.get(aggregators_db, cur.0);
            if get_count.is_ok() {
                let count = deserialize_u8!(try_unwrap!(get_count));
                total_count += count;
                total_sum += val * u64::from(count);
            }
            exist = ptx_cur.next()?;
        }
        Ok(check_nan_f64!(total_sum as f64 / total_count as f64) as u64)
    }
}
