use crate::types::{DozerDuration, DozerPoint, Field};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, Timelike, Utc};
use geo::Point;
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

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Field::String(value.to_string())
    }
}

impl From<i8> for Field {
    fn from(value: i8) -> Self {
        Field::Int(value.into())
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

impl From<u8> for Field {
    fn from(value: u8) -> Self {
        Field::UInt(value.into())
    }
}

impl From<u16> for Field {
    fn from(value: u16) -> Self {
        Field::UInt(value.into())
    }
}

impl From<u32> for Field {
    fn from(value: u32) -> Self {
        Field::UInt(value.into())
    }
}

impl From<u64> for Field {
    fn from(value: u64) -> Self {
        Field::UInt(value)
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
        Field::Timestamp(DateTime::from_naive_utc_and_offset(value, Utc.fix()))
    }
}

impl From<DateTime<FixedOffset>> for Field {
    fn from(value: DateTime<FixedOffset>) -> Self {
        Field::Timestamp(value)
    }
}

impl From<NaiveDate> for Field {
    fn from(value: NaiveDate) -> Self {
        Field::Date(value)
    }
}

impl From<NaiveTime> for Field {
    fn from(value: NaiveTime) -> Self {
        Field::UInt(value.nanosecond().into())
    }
}

impl From<Point<OrderedFloat<f64>>> for Field {
    fn from(value: Point<OrderedFloat<f64>>) -> Self {
        Field::Point(DozerPoint(value))
    }
}

impl From<Point> for Field {
    fn from(value: Point) -> Self {
        let v = DozerPoint::from((value.x(), value.y()));
        Field::Point(v)
    }
}

impl From<DozerDuration> for Field {
    fn from(value: DozerDuration) -> Self {
        let v = DozerDuration(value.0, value.1);
        Field::Duration(v)
    }
}
