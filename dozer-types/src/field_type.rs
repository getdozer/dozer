use crate::types::Field;
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
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
        Field::Float(value.into())
    }
}

impl From<f64> for Field {
    fn from(value: f64) -> Self {
        Field::Float(value)
    }
}

impl From<Decimal> for Field {
    fn from(value: Decimal) -> Self {
        Field::Decimal(value)
    }
}

impl From<NaiveDateTime> for Field {
    fn from(value: NaiveDateTime) -> Self {
        Field::Timestamp(DateTime::<Utc>::from_utc(value, Utc))
    }
}

impl From<DateTime<FixedOffset>> for Field {
    fn from(value: DateTime<FixedOffset>) -> Self {
        Field::Timestamp(DateTime::<Utc>::from_utc(value.naive_utc(), Utc))
    }
}
