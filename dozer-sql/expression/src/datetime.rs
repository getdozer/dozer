use crate::arg_utils::{extract_timestamp, extract_uint, validate_arg_type};
use crate::error::Error;
use crate::execution::{Expression, ExpressionType};

use dozer_types::chrono::{DateTime, Datelike, FixedOffset, Offset, Timelike, Utc};
use dozer_types::types::Record;
use dozer_types::types::{DozerDuration, Field, FieldType, Schema, TimeUnit};
use num_traits::ToPrimitive;
use sqlparser::ast::DateTimeField;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum DateTimeFunctionType {
    Extract {
        field: sqlparser::ast::DateTimeField,
    },
    Interval {
        field: sqlparser::ast::DateTimeField,
    },
    Now,
}

impl Display for DateTimeFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimeFunctionType::Extract { field } => {
                f.write_str(format!("EXTRACT {field}").as_str())
            }
            DateTimeFunctionType::Interval { field } => {
                f.write_str(format!("INTERVAL {field}").as_str())
            }
            DateTimeFunctionType::Now => f.write_str("NOW".to_string().as_str()),
        }
    }
}

pub(crate) fn get_datetime_function_type(
    function: &DateTimeFunctionType,
    arg: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    validate_arg_type(
        arg,
        vec![
            FieldType::Date,
            FieldType::Timestamp,
            FieldType::Duration,
            FieldType::String,
            FieldType::Text,
        ],
        schema,
        function,
        0,
    )?;
    match function {
        DateTimeFunctionType::Extract { field: _ } => Ok(ExpressionType::new(
            FieldType::Int,
            false,
            dozer_types::types::SourceDefinition::Dynamic,
            false,
        )),
        DateTimeFunctionType::Interval { field: _ } => Ok(ExpressionType::new(
            FieldType::Duration,
            false,
            dozer_types::types::SourceDefinition::Dynamic,
            false,
        )),
        DateTimeFunctionType::Now => Ok(ExpressionType::new(
            FieldType::Timestamp,
            false,
            dozer_types::types::SourceDefinition::Dynamic,
            false,
        )),
    }
}

impl DateTimeFunctionType {
    pub(crate) fn new(name: &str) -> Option<DateTimeFunctionType> {
        match name {
            "now" => Some(DateTimeFunctionType::Now),
            _ => None,
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        arg: &Expression,
        record: &Record,
    ) -> Result<Field, Error> {
        match self {
            DateTimeFunctionType::Extract { field } => {
                evaluate_date_part(schema, field, arg, record)
            }
            DateTimeFunctionType::Interval { field } => {
                evaluate_interval(schema, field, arg, record)
            }
            DateTimeFunctionType::Now => self.evaluate_now(),
        }
    }

    pub(crate) fn evaluate_now(&self) -> Result<Field, Error> {
        Ok(Field::Timestamp(DateTime::<FixedOffset>::from(Utc::now())))
    }
}

pub(crate) fn evaluate_date_part(
    schema: &Schema,
    field: &sqlparser::ast::DateTimeField,
    arg: &Expression,
    record: &Record,
) -> Result<Field, Error> {
    let value = arg.evaluate(record, schema)?;

    let ts = extract_timestamp(value, DateTimeFunctionType::Extract { field: *field }, 0)?;

    match field {
        DateTimeField::Dow => ts.weekday().num_days_from_monday().to_i64(),
        DateTimeField::Day => ts.day().to_i64(),
        DateTimeField::Month => ts.month().to_i64(),
        DateTimeField::Year => ts.year().to_i64(),
        DateTimeField::Hour => ts.hour().to_i64(),
        DateTimeField::Minute => ts.minute().to_i64(),
        DateTimeField::Second => ts.second().to_i64(),
        DateTimeField::Millisecond | DateTimeField::Milliseconds => ts.timestamp_millis().to_i64(),
        DateTimeField::Microsecond | DateTimeField::Microseconds => ts.timestamp_micros().to_i64(),
        DateTimeField::Nanoseconds | DateTimeField::Nanosecond => ts.timestamp_nanos().to_i64(),
        DateTimeField::Quarter => ts.month0().to_i64().map(|m| m / 3 + 1),
        DateTimeField::Epoch => ts.timestamp().to_i64(),
        DateTimeField::Week => ts.iso_week().week().to_i64(),
        DateTimeField::Century => ts.year().to_i64().map(|y| (y as f64 / 100.0).ceil() as i64),
        DateTimeField::Decade => ts.year().to_i64().map(|y| (y as f64 / 10.0).ceil() as i64),
        DateTimeField::Doy => ts.ordinal().to_i64(),
        DateTimeField::Timezone => ts.offset().fix().local_minus_utc().to_i64(),
        DateTimeField::Isodow
        | DateTimeField::Isoyear
        | DateTimeField::Julian
        | DateTimeField::Millenium
        | DateTimeField::Millennium
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute
        | DateTimeField::Date
        | DateTimeField::NoDateTime => None,
    }
    .ok_or(Error::UnsupportedExtract(*field))
    .map(Field::Int)
}

pub(crate) fn evaluate_interval(
    schema: &Schema,
    field: &sqlparser::ast::DateTimeField,
    arg: &Expression,
    record: &Record,
) -> Result<Field, Error> {
    let value = arg.evaluate(record, schema)?;
    let dur = extract_uint(value, DateTimeFunctionType::Interval { field: *field }, 0)?;

    match field {
        DateTimeField::Second => Ok(Field::Duration(DozerDuration(
            std::time::Duration::from_secs(dur),
            TimeUnit::Seconds,
        ))),
        DateTimeField::Millisecond | DateTimeField::Milliseconds => {
            Ok(Field::Duration(DozerDuration(
                std::time::Duration::from_millis(dur),
                TimeUnit::Milliseconds,
            )))
        }
        DateTimeField::Microsecond | DateTimeField::Microseconds => {
            Ok(Field::Duration(DozerDuration(
                std::time::Duration::from_micros(dur),
                TimeUnit::Microseconds,
            )))
        }
        DateTimeField::Nanoseconds | DateTimeField::Nanosecond => Ok(Field::Duration(
            DozerDuration(std::time::Duration::from_nanos(dur), TimeUnit::Nanoseconds),
        )),
        DateTimeField::Isodow
        | DateTimeField::Timezone
        | DateTimeField::Dow
        | DateTimeField::Isoyear
        | DateTimeField::Julian
        | DateTimeField::Millenium
        | DateTimeField::Millennium
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute
        | DateTimeField::Date
        | DateTimeField::NoDateTime
        | DateTimeField::Day
        | DateTimeField::Month
        | DateTimeField::Year
        | DateTimeField::Hour
        | DateTimeField::Minute
        | DateTimeField::Quarter
        | DateTimeField::Epoch
        | DateTimeField::Week
        | DateTimeField::Century
        | DateTimeField::Decade
        | DateTimeField::Doy => Err(Error::UnsupportedInterval(*field)),
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::ArbitraryDateTime;

    use super::*;

    use proptest::prelude::*;

    #[test]
    fn test_time() {
        proptest!(
            ProptestConfig::with_cases(1000),
            move |(datetime: ArbitraryDateTime)| {
                test_date_parts(datetime)
        });
    }

    fn test_date_parts(datetime: ArbitraryDateTime) {
        let row = Record::new(vec![]);

        let date_parts = vec![
            (
                DateTimeField::Dow,
                datetime
                    .0
                    .weekday()
                    .num_days_from_monday()
                    .to_i64()
                    .unwrap(),
            ),
            (DateTimeField::Year, datetime.0.year().to_i64().unwrap()),
            (DateTimeField::Month, datetime.0.month().to_i64().unwrap()),
            (DateTimeField::Hour, 0),
            (DateTimeField::Second, 0),
            (
                DateTimeField::Quarter,
                datetime.0.month0().to_i64().map(|m| m / 3 + 1).unwrap(),
            ),
        ];

        let v = Expression::Literal(Field::Date(datetime.0.date_naive()));

        for (part, value) in date_parts {
            let result = evaluate_date_part(&Schema::default(), &part, &v, &row).unwrap();
            assert_eq!(result, Field::Int(value));
        }
    }
}
