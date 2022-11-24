use crate::types::Field;
use chrono::{DateTime, FixedOffset, NaiveDateTime, Offset, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;

impl From<bool> for Field {
    fn from(value: bool) -> Self {
        Field::Boolean(value)
    }
}

impl From<String> for Field {
    fn from(value: String) -> Self {
        Field::String(value)
    }
}

impl From<i16> for Field {
    fn from(value: i16) -> Self {
        Field::Int(value.into())
    }
}

impl From<i32> for Field {
    fn from(value: i32) -> Self {
        Field::Int(value.into())
    }
}

impl From<i64> for Field {
    fn from(value: i64) -> Self {
        Field::Int(value)
    }
}

impl From<f32> for Field {
    fn from(value: f32) -> Self {
        Field::Float(OrderedFloat(value.into()))
    }
}

impl From<f64> for Field {
    fn from(value: f64) -> Self {
        Field::Float(OrderedFloat(value))
    }
}

impl From<Decimal> for Field {
    fn from(value: Decimal) -> Self {
        Field::Decimal(value)
    }
}

impl From<NaiveDateTime> for Field {
    fn from(value: NaiveDateTime) -> Self {
        Field::Timestamp(DateTime::from_utc(value, Utc.fix()))
    }
}

impl From<DateTime<FixedOffset>> for Field {
    fn from(value: DateTime<FixedOffset>) -> Self {
        Field::Timestamp(value)
    }
}
