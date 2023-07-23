use crate::pipeline::errors::PipelineError::{
    InvalidFunction, InvalidFunctionArgument, InvalidFunctionArgumentType,
};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::datetime::PipelineError::InvalidValue;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};

use dozer_types::chrono::{DateTime, Datelike, FixedOffset, Offset, Timelike, Utc};
use dozer_types::types::{DozerDuration, Field, FieldType, ProcessorRecord, Schema, TimeUnit};
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
) -> Result<ExpressionType, PipelineError> {
    let return_type = arg.get_type(schema)?.return_type;
    if return_type != FieldType::Date
        && return_type != FieldType::Timestamp
        && return_type != FieldType::Duration
        && return_type != FieldType::String
    {
        return Err(InvalidFunctionArgumentType(
            function.to_string(),
            return_type,
            FieldTypes::new(vec![
                FieldType::Date,
                FieldType::Timestamp,
                FieldType::Duration,
                FieldType::String,
            ]),
            0,
        ));
    }
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
    pub(crate) fn new(name: &str) -> Result<DateTimeFunctionType, PipelineError> {
        match name {
            "now" => Ok(DateTimeFunctionType::Now),
            _ => Err(InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        arg: &Expression,
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
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

    pub(crate) fn evaluate_now(&self) -> Result<Field, PipelineError> {
        Ok(Field::Timestamp(DateTime::<FixedOffset>::from(Utc::now())))
    }
}

pub(crate) fn evaluate_date_part(
    schema: &Schema,
    field: &sqlparser::ast::DateTimeField,
    arg: &Expression,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;

    let ts = match value {
        Field::Timestamp(ts) => Ok(ts),
        Field::Date(d) => d
            .and_hms_milli_opt(0, 0, 0, 0)
            .map(|ts| DateTime::from_utc(ts, Utc.fix()))
            .ok_or(InvalidValue(format!(
                "Unable to cast date {d} to timestamp"
            ))),
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => {
            return Err(InvalidFunctionArgument(
                DateTimeFunctionType::Extract { field: *field }.to_string(),
                value,
                0,
            ))
        }
    }?;

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
    .ok_or(PipelineError::InvalidOperandType(format!(
        "Unable to extract date part {field} from {value}"
    )))
    .map(Field::Int)
}

pub(crate) fn evaluate_interval(
    schema: &Schema,
    field: &sqlparser::ast::DateTimeField,
    arg: &Expression,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    let dur = value.to_duration()?.unwrap().0.as_nanos();

    match field {
        DateTimeField::Second => Ok(Field::Duration(DozerDuration(
            std::time::Duration::from_secs(dur as u64),
            TimeUnit::Seconds,
        ))),
        DateTimeField::Millisecond | DateTimeField::Milliseconds => {
            Ok(Field::Duration(DozerDuration(
                std::time::Duration::from_millis(dur as u64),
                TimeUnit::Milliseconds,
            )))
        }
        DateTimeField::Microsecond | DateTimeField::Microseconds => {
            Ok(Field::Duration(DozerDuration(
                std::time::Duration::from_micros(dur as u64),
                TimeUnit::Microseconds,
            )))
        }
        DateTimeField::Nanoseconds | DateTimeField::Nanosecond => {
            Ok(Field::Duration(DozerDuration(
                std::time::Duration::from_nanos(dur as u64),
                TimeUnit::Nanoseconds,
            )))
        }
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
        | DateTimeField::Doy => Err(PipelineError::InvalidOperandType(format!(
            "Unable to extract date part {field} from {value}"
        ))),
    }
}
