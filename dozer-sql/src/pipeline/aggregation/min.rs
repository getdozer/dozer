use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidOperandType;
use crate::{
    deserialize_u8, field_extract_date, field_extract_decimal, field_extract_f64,
    field_extract_i64, field_extract_timestamp, to_bytes, try_unwrap,
};

use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::{Date, Decimal, Float, Int, Timestamp};
use dozer_types::types::{Field, FieldType, DATE_FORMAT};

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use dozer_types::deserialize;
use std::string::ToString;

pub struct MinAggregator {}
const AGGREGATOR_NAME: &str = "MIN";

impl MinAggregator {
    const _AGGREGATOR_ID: u32 = 0x04;

    pub(crate) fn get_return_type(from: FieldType) -> FieldType {
        match from {
            FieldType::Int => FieldType::Int,
            FieldType::Float => FieldType::Float,
            FieldType::Decimal => FieldType::Decimal,
            FieldType::Timestamp => FieldType::Timestamp,
            FieldType::Date => FieldType::Date,
            _ => from,
        }
    }

    pub(crate) fn _get_type() -> u32 {
        MinAggregator::_AGGREGATOR_ID
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

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_i64_min(ptx, aggregators_db));
                if minimum == i64::MAX {
                    Ok(AggregationResult::new(
                        Field::Null,
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);

                // Calculate average
                let minimum = try_unwrap!(Self::calc_f64_min(ptx, aggregators_db));
                if minimum == f64::MAX {
                    Ok(AggregationResult::new(
                        Field::Null,
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Decimal(_d) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_decimal!(&new, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_decimal_min(ptx, aggregators_db));
                if minimum == dozer_types::rust_decimal::Decimal::MAX {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.serialize().as_slice(), return_type),
                        Some(Vec::from(minimum.serialize())),
                    ))
                }
            }
            Timestamp(_t) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_timestamp!(&new, AGGREGATOR_NAME)
                    .timestamp_millis()
                    .to_le_bytes();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_timestamp_min(ptx, aggregators_db));
                let max_datetime: DateTime<FixedOffset> =
                    DateTime::from(DateTime::<FixedOffset>::MAX_UTC);
                if minimum == max_datetime {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(
                            minimum.timestamp_millis().to_le_bytes().as_slice(),
                            return_type,
                        ),
                        Some(Vec::from(minimum.timestamp_millis().to_le_bytes())),
                    ))
                }
            }
            Date(_d) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_date!(&new, AGGREGATOR_NAME).to_string();
                Self::update_aggregator_db(new_val.as_bytes(), 1, false, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_date_min(ptx, aggregators_db));
                let max_date = NaiveDate::MAX;
                if minimum == max_date {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.to_string().as_bytes(), return_type),
                        Some(Vec::from(minimum.to_string().as_bytes())),
                    ))
                }
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

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_i64_min(ptx, aggregators_db));
                if minimum == i64::MAX {
                    Ok(AggregationResult::new(
                        Field::Null,
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_f64!(&new, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(new_val), 1, false, ptx, aggregators_db);
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_f64_min(ptx, aggregators_db));
                if minimum == f64::MAX {
                    Ok(AggregationResult::new(
                        Field::Null,
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Decimal(_d) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_decimal!(&new, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);
                let old_val = field_extract_decimal!(&old, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_decimal_min(ptx, aggregators_db));
                if minimum == dozer_types::rust_decimal::Decimal::MAX {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.serialize().as_slice(), return_type),
                        Some(Vec::from(minimum.serialize())),
                    ))
                }
            }
            Timestamp(_t) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_timestamp!(&new, AGGREGATOR_NAME)
                    .timestamp_millis()
                    .to_le_bytes();
                Self::update_aggregator_db(new_val.as_slice(), 1, false, ptx, aggregators_db);
                let old_val = field_extract_timestamp!(&old, AGGREGATOR_NAME)
                    .timestamp_millis()
                    .to_le_bytes();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_timestamp_min(ptx, aggregators_db));
                let max_datetime: DateTime<FixedOffset> =
                    DateTime::from(DateTime::<FixedOffset>::MAX_UTC);
                if minimum == max_datetime {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(
                            minimum.timestamp_millis().to_le_bytes().as_slice(),
                            return_type,
                        ),
                        Some(Vec::from(minimum.timestamp_millis().to_le_bytes())),
                    ))
                }
            }
            Date(_d) => {
                // Update aggregators_db with new val and its occurrence
                let new_val = field_extract_date!(&new, AGGREGATOR_NAME).to_string();
                Self::update_aggregator_db(new_val.as_bytes(), 1, false, ptx, aggregators_db);
                let old_val = field_extract_date!(&old, AGGREGATOR_NAME).to_string();
                Self::update_aggregator_db(old_val.as_bytes(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_date_min(ptx, aggregators_db));
                let max_date = NaiveDate::MAX;
                if minimum == max_date {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.to_string().as_bytes(), return_type),
                        Some(Vec::from(minimum.to_string().as_bytes())),
                    ))
                }
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

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_i64_min(ptx, aggregators_db));
                if minimum == i64::MAX {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Float(_f) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_f64!(&old, AGGREGATOR_NAME);
                Self::update_aggregator_db(to_bytes!(old_val), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_f64_min(ptx, aggregators_db));
                if minimum == f64::MAX {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(&minimum.to_le_bytes(), return_type),
                        Some(Vec::from(minimum.to_le_bytes())),
                    ))
                }
            }
            Decimal(_d) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_decimal!(&old, AGGREGATOR_NAME).serialize();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_decimal_min(ptx, aggregators_db));
                if minimum == dozer_types::rust_decimal::Decimal::MAX {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.serialize().as_slice(), return_type),
                        Some(Vec::from(minimum.serialize())),
                    ))
                }
            }
            Timestamp(_t) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_timestamp!(&old, AGGREGATOR_NAME)
                    .timestamp_millis()
                    .to_le_bytes();
                Self::update_aggregator_db(old_val.as_slice(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_timestamp_min(ptx, aggregators_db));
                let max_datetime: DateTime<FixedOffset> =
                    DateTime::from(DateTime::<FixedOffset>::MAX_UTC);
                if minimum == max_datetime {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(
                            minimum.timestamp_millis().to_le_bytes().as_slice(),
                            return_type,
                        ),
                        Some(Vec::from(minimum.timestamp_millis().to_le_bytes())),
                    ))
                }
            }
            Date(_d) => {
                // Update aggregators_db with new val and its occurrence
                let old_val = field_extract_date!(&old, AGGREGATOR_NAME).to_string();
                Self::update_aggregator_db(old_val.as_bytes(), 1, true, ptx, aggregators_db);

                // Calculate minimum
                let minimum = try_unwrap!(Self::calc_date_min(ptx, aggregators_db));
                let max_date = NaiveDate::MAX;
                if minimum == max_date {
                    Ok(AggregationResult::new(Field::Null, None))
                } else {
                    Ok(AggregationResult::new(
                        Self::get_value(minimum.to_string().as_bytes(), return_type),
                        Some(Vec::from(minimum.to_string().as_bytes())),
                    ))
                }
            }
            _ => Err(InvalidOperandType(AGGREGATOR_NAME.to_string())),
        }
    }

    pub(crate) fn get_value(f: &[u8], from: FieldType) -> Field {
        match from {
            FieldType::Int => Int(i64::from_le_bytes(deserialize!(f))),
            FieldType::Float => Float(OrderedFloat(f64::from_le_bytes(deserialize!(f)))),
            FieldType::Decimal => Decimal(dozer_types::rust_decimal::Decimal::deserialize(
                deserialize!(f),
            )),
            FieldType::Timestamp => Timestamp(DateTime::from(
                Utc.timestamp_millis(i64::from_le_bytes(deserialize!(f))),
            )),
            FieldType::Date => Date(
                NaiveDate::parse_from_str(
                    String::from_utf8(deserialize!(f)).unwrap().as_ref(),
                    DATE_FORMAT,
                )
                .unwrap(),
            ),
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

    fn calc_f64_min(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<f64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut minimum = f64::MAX;

        // get first to get the minimum
        if ptx_cur.first()? {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            minimum = f64::from_le_bytes(deserialize!(cur.0));
        }
        Ok(minimum)
    }

    fn calc_decimal_min(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<dozer_types::rust_decimal::Decimal, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut minimum = dozer_types::rust_decimal::Decimal::MAX;

        // get first to get the minimum
        if ptx_cur.first()? {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            minimum = dozer_types::rust_decimal::Decimal::deserialize(deserialize!(cur.0));
        }
        Ok(minimum)
    }

    fn calc_timestamp_min(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<DateTime<FixedOffset>, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut minimum = DateTime::<FixedOffset>::MAX_UTC;

        // get first to get the minimum
        if ptx_cur.first()? {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            minimum = Utc.timestamp_millis(i64::from_le_bytes(deserialize!(cur.0)));
        }
        Ok(DateTime::from(minimum))
    }

    fn calc_date_min(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<NaiveDate, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut minimum = NaiveDate::MAX;

        // get first to get the minimum
        if ptx_cur.first()? {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            minimum = NaiveDate::parse_from_str(
                String::from_utf8(deserialize!(cur.0)).unwrap().as_ref(),
                DATE_FORMAT,
            )
            .unwrap();
        }
        Ok(minimum)
    }

    fn calc_i64_min(
        ptx: &mut PrefixTransaction,
        aggregators_db: &Database,
    ) -> Result<i64, PipelineError> {
        let ptx_cur = ptx.open_cursor(aggregators_db)?;
        let mut minimum = i64::MAX;

        // get first to get the minimum
        if ptx_cur.first()? {
            let cur = try_unwrap!(ptx_cur.read()).unwrap();
            minimum = i64::from_le_bytes(deserialize!(cur.0));
        }
        Ok(minimum)
    }
}
