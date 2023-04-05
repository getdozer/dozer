use crate::errors::types::{DeserializationError, TypeError};
use crate::types::{DozerDuration, DozerPoint, TimeUnit};
#[allow(unused_imports)]
use chrono::{DateTime, Datelike, FixedOffset, LocalResult, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::time::Duration;

pub const DATE_FORMAT: &str = "%Y-%m-%d";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, Hash)]
pub enum Field {
    UInt(u64),
    U128(u128),
    Int(i64),
    I128(i128),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    String(String),
    Text(String),
    Binary(Vec<u8>),
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Bson(Vec<u8>),
    Point(DozerPoint),
    Duration(DozerDuration),
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum FieldBorrow<'a> {
    UInt(u64),
    U128(u128),
    Int(i64),
    I128(i128),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    String(&'a str),
    Text(&'a str),
    Binary(&'a [u8]),
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Bson(&'a [u8]),
    Point(DozerPoint),
    Duration(DozerDuration),
    Null,
}

impl Field {
    pub(crate) fn data_encoding_len(&self) -> usize {
        match self {
            Field::UInt(_) => 8,
            Field::U128(_) => 16,
            Field::Int(_) => 8,
            Field::I128(_) => 16,
            Field::Float(_) => 8,
            Field::Boolean(_) => 1,
            Field::String(s) => s.len(),
            Field::Text(s) => s.len(),
            Field::Binary(b) => b.len(),
            Field::Decimal(_) => 16,
            Field::Timestamp(_) => 8,
            Field::Date(_) => 10,
            Field::Bson(b) => b.len(),
            Field::Point(_p) => 16,
            Field::Duration(_) => 32,
            Field::Null => 0,
        }
    }

    pub(crate) fn encode_data(&self) -> Cow<[u8]> {
        match self {
            Field::UInt(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::U128(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::Int(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::I128(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::Float(f) => Cow::Owned(f.to_be_bytes().into()),
            Field::Boolean(b) => Cow::Owned(if *b { [1] } else { [0] }.into()),
            Field::String(s) => Cow::Borrowed(s.as_bytes()),
            Field::Text(s) => Cow::Borrowed(s.as_bytes()),
            Field::Binary(b) => Cow::Borrowed(b.as_slice()),
            Field::Decimal(d) => Cow::Owned(d.serialize().into()),
            Field::Timestamp(t) => Cow::Owned(t.timestamp_millis().to_be_bytes().into()),
            Field::Date(t) => Cow::Owned(t.to_string().into()),
            Field::Bson(b) => Cow::Borrowed(b),
            Field::Point(p) => Cow::Owned(p.to_bytes().into()),
            Field::Duration(d) => Cow::Owned(d.to_bytes().into()),
            Field::Null => Cow::Owned([].into()),
        }
    }

    pub fn encode_buf(&self, destination: &mut [u8]) {
        let prefix = self.get_type_prefix();
        let data = self.encode_data();
        destination[0] = prefix;
        destination[1..].copy_from_slice(&data);
    }

    pub fn encoding_len(&self) -> usize {
        self.data_encoding_len() + 1
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut result = vec![0; self.encoding_len()];
        self.encode_buf(&mut result);
        result
    }

    pub fn borrow(&self) -> FieldBorrow {
        match self {
            Field::UInt(i) => FieldBorrow::UInt(*i),
            Field::U128(i) => FieldBorrow::U128(*i),
            Field::Int(i) => FieldBorrow::Int(*i),
            Field::I128(i) => FieldBorrow::I128(*i),
            Field::Float(f) => FieldBorrow::Float(*f),
            Field::Boolean(b) => FieldBorrow::Boolean(*b),
            Field::String(s) => FieldBorrow::String(s),
            Field::Text(s) => FieldBorrow::Text(s),
            Field::Binary(b) => FieldBorrow::Binary(b),
            Field::Decimal(d) => FieldBorrow::Decimal(*d),
            Field::Timestamp(t) => FieldBorrow::Timestamp(*t),
            Field::Date(t) => FieldBorrow::Date(*t),
            Field::Bson(b) => FieldBorrow::Bson(b),
            Field::Point(p) => FieldBorrow::Point(*p),
            Field::Duration(d) => FieldBorrow::Duration(*d),
            Field::Null => FieldBorrow::Null,
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Field, DeserializationError> {
        Self::decode_borrow(buf).map(|field| field.to_owned())
    }

    pub fn decode_borrow(buf: &[u8]) -> Result<FieldBorrow, DeserializationError> {
        let first_byte = *buf.first().ok_or(DeserializationError::EmptyInput)?;
        let val = &buf[1..];
        match first_byte {
            0 => Ok(FieldBorrow::UInt(u64::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            1 => Ok(FieldBorrow::U128(u128::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            2 => Ok(FieldBorrow::Int(i64::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            3 => Ok(FieldBorrow::I128(i128::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            4 => Ok(FieldBorrow::Float(OrderedFloat(f64::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            )))),
            5 => Ok(FieldBorrow::Boolean(val[0] == 1)),
            6 => Ok(FieldBorrow::String(std::str::from_utf8(val)?)),
            7 => Ok(FieldBorrow::Text(std::str::from_utf8(val)?)),
            8 => Ok(FieldBorrow::Binary(val)),
            9 => Ok(FieldBorrow::Decimal(Decimal::deserialize(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            10 => {
                let timestamp = Utc.timestamp_millis_opt(i64::from_be_bytes(
                    val.try_into()
                        .map_err(|_| DeserializationError::BadDataLength)?,
                ));

                match timestamp {
                    LocalResult::Single(v) => Ok(FieldBorrow::Timestamp(DateTime::from(v))),
                    LocalResult::Ambiguous(_, _) => Err(DeserializationError::Custom(Box::new(
                        TypeError::AmbiguousTimestamp,
                    ))),
                    LocalResult::None => Err(DeserializationError::Custom(Box::new(
                        TypeError::InvalidTimestamp,
                    ))),
                }
            }
            11 => Ok(FieldBorrow::Date(NaiveDate::parse_from_str(
                std::str::from_utf8(val)?,
                DATE_FORMAT,
            )?)),
            12 => Ok(FieldBorrow::Bson(val)),
            13 => Ok(FieldBorrow::Point(
                DozerPoint::from_bytes(val).map_err(|_| DeserializationError::BadDataLength)?,
            )),
            14 => Ok(FieldBorrow::Duration(
                DozerDuration::from_bytes(val).map_err(|_| DeserializationError::BadDataLength)?,
            )),
            15 => Ok(FieldBorrow::Null),
            other => Err(DeserializationError::UnrecognisedFieldType(other)),
        }
    }

    fn get_type_prefix(&self) -> u8 {
        match self {
            Field::UInt(_) => 0,
            Field::U128(_) => 1,
            Field::Int(_) => 2,
            Field::I128(_) => 3,
            Field::Float(_) => 4,
            Field::Boolean(_) => 5,
            Field::String(_) => 6,
            Field::Text(_) => 7,
            Field::Binary(_) => 8,
            Field::Decimal(_) => 9,
            Field::Timestamp(_) => 10,
            Field::Date(_) => 11,
            Field::Bson(_) => 12,
            Field::Point(_) => 13,
            Field::Duration(_) => 32,
            Field::Null => 15,
        }
    }

    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Field::UInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_u128(&self) -> Option<u128> {
        match self {
            Field::U128(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Field::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_i128(&self) -> Option<i128> {
        match self {
            Field::I128(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Field::Float(f) => Some(f.0),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Field::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Field::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Field::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Field::Binary(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Field::Decimal(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<DateTime<FixedOffset>> {
        match self {
            Field::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Field::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_bson(&self) -> Option<&[u8]> {
        match self {
            Field::Bson(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_point(&self) -> Option<DozerPoint> {
        match self {
            Field::Point(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_duration(&self) -> Option<DozerDuration> {
        match self {
            Field::UInt(d) => Some(DozerDuration(
                Duration::from_nanos(*d),
                TimeUnit::Nanoseconds,
            )),
            Field::U128(d) => Some(DozerDuration(
                Duration::from_nanos(*d as u64),
                TimeUnit::Nanoseconds,
            )),
            Field::Int(d) => Some(DozerDuration(
                Duration::from_nanos(*d as u64),
                TimeUnit::Nanoseconds,
            )),
            Field::I128(d) => Some(DozerDuration(
                Duration::from_nanos(*d as u64),
                TimeUnit::Nanoseconds,
            )),
            Field::Duration(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Field::Null => Some(()),
            _ => None,
        }
    }

    pub fn to_uint(&self) -> Option<u64> {
        match self {
            Field::UInt(u) => Some(*u),
            Field::U128(u) => u64::from_u128(*u),
            Field::Int(i) => u64::from_i64(*i),
            Field::I128(i) => u64::from_i128(*i),
            Field::Float(f) => u64::from_f64(f.0),
            Field::Decimal(d) => d.to_u64(),
            Field::String(s) => s.parse::<u64>().ok(),
            Field::Text(s) => s.parse::<u64>().ok(),
            Field::Null => Some(0_u64),
            _ => None,
        }
    }

    pub fn to_u128(&self) -> Option<u128> {
        match self {
            Field::UInt(u) => u128::from_u64(*u),
            Field::U128(u) => Some(*u),
            Field::Int(i) => u128::from_i64(*i),
            Field::I128(i) => u128::from_i128(*i),
            Field::Float(f) => u128::from_f64(f.0),
            Field::Decimal(d) => d.to_u128(),
            Field::String(s) => s.parse::<u128>().ok(),
            Field::Text(s) => s.parse::<u128>().ok(),
            Field::Null => Some(0_u128),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            Field::UInt(u) => i64::from_u64(*u),
            Field::U128(u) => i64::from_u128(*u),
            Field::Int(i) => Some(*i),
            Field::I128(i) => i64::from_i128(*i),
            Field::Float(f) => i64::from_f64(f.0),
            Field::Decimal(d) => d.to_i64(),
            Field::String(s) => s.parse::<i64>().ok(),
            Field::Text(s) => s.parse::<i64>().ok(),
            Field::Null => Some(0_i64),
            _ => None,
        }
    }

    pub fn to_i128(&self) -> Option<i128> {
        match self {
            Field::UInt(u) => i128::from_u64(*u),
            Field::U128(u) => i128::from_u128(*u),
            Field::Int(i) => i128::from_i64(*i),
            Field::I128(i) => Some(*i),
            Field::Float(f) => i128::from_f64(f.0),
            Field::Decimal(d) => d.to_i128(),
            Field::String(s) => s.parse::<i128>().ok(),
            Field::Text(s) => s.parse::<i128>().ok(),
            Field::Null => Some(0_i128),
            _ => None,
        }
    }

    pub fn to_float(&self) -> Option<f64> {
        match self {
            Field::UInt(u) => f64::from_u64(*u),
            Field::U128(u) => f64::from_u128(*u),
            Field::Int(i) => f64::from_i64(*i),
            Field::I128(i) => f64::from_i128(*i),
            Field::Float(f) => Some(f.0),
            Field::Decimal(d) => d.to_f64(),
            Field::String(s) => s.parse::<f64>().ok(),
            Field::Text(s) => s.parse::<f64>().ok(),
            Field::Null => Some(0_f64),
            _ => None,
        }
    }

    pub fn to_boolean(&self) -> Option<bool> {
        match self {
            Field::UInt(u) => Some(*u > 0_u64),
            Field::U128(u) => Some(*u > 0_u128),
            Field::Int(i) => Some(*i > 0_i64),
            Field::I128(i) => Some(*i > 0_i128),
            Field::Float(i) => Some(i.0 > 0_f64),
            Field::Decimal(i) => Some(i.gt(&Decimal::from(0_u64))),
            Field::Boolean(b) => Some(*b),
            Field::String(s) => s.parse::<bool>().ok(),
            Field::Text(s) => s.parse::<bool>().ok(),
            Field::Null => Some(false),
            _ => None,
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            Field::UInt(u) => Some(format!("{u}")),
            Field::U128(u) => Some(format!("{u}")),
            Field::Int(i) => Some(format!("{i}")),
            Field::I128(i) => Some(format!("{i}")),
            Field::Float(f) => Some(format!("{f}")),
            Field::Decimal(d) => Some(format!("{d}")),
            Field::Boolean(i) => Some(if *i {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }),
            Field::String(s) => Some(s.to_owned()),
            Field::Text(t) => Some(t.to_owned()),
            Field::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Field::Timestamp(t) => Some(t.to_rfc3339()),
            Field::Binary(b) => Some(format!("{b:X?}")),
            Field::Null => Some("".to_string()),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<String> {
        match self {
            Field::UInt(u) => Some(format!("{u}")),
            Field::U128(u) => Some(format!("{u}")),
            Field::Int(i) => Some(format!("{i}")),
            Field::I128(i) => Some(format!("{i}")),
            Field::Float(f) => Some(format!("{f}")),
            Field::Decimal(d) => Some(format!("{d}")),
            Field::Boolean(i) => Some(if *i {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }),
            Field::String(s) => Some(s.to_owned()),
            Field::Text(t) => Some(t.to_owned()),
            Field::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Field::Timestamp(t) => Some(t.to_rfc3339()),
            Field::Binary(b) => Some(format!("{b:X?}")),
            Field::Null => Some("".to_string()),
            _ => None,
        }
    }

    pub fn to_binary(&self) -> Option<&[u8]> {
        match self {
            Field::Binary(b) => Some(b),
            _ => None,
        }
    }

    pub fn to_decimal(&self) -> Option<Decimal> {
        match self {
            Field::UInt(u) => Decimal::from_u64(*u),
            Field::U128(u) => Decimal::from_u128(*u),
            Field::Int(i) => Decimal::from_i64(*i),
            Field::I128(i) => Decimal::from_i128(*i),
            Field::Float(f) => Decimal::from_f64_retain(f.0),
            Field::Decimal(d) => Some(*d),
            Field::String(s) => Decimal::from_str_exact(s).ok(),
            Field::Null => Some(Decimal::from(0)),
            _ => None,
        }
    }

    pub fn to_timestamp(&self) -> Result<Option<DateTime<FixedOffset>>, TypeError> {
        match self {
            Field::String(s) => Ok(DateTime::parse_from_rfc3339(s.as_str()).ok()),
            Field::Timestamp(t) => Ok(Some(*t)),
            Field::Null => match Utc.timestamp_millis_opt(0) {
                LocalResult::None => Err(TypeError::InvalidTimestamp),
                LocalResult::Single(v) => Ok(Some(DateTime::from(v))),
                LocalResult::Ambiguous(_, _) => Err(TypeError::AmbiguousTimestamp),
            },
            _ => Ok(None),
        }
    }

    pub fn to_date(&self) -> Result<Option<NaiveDate>, TypeError> {
        match self {
            Field::String(s) => Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()),
            Field::Date(d) => Ok(Some(*d)),
            Field::Null => match Utc.timestamp_millis_opt(0) {
                LocalResult::None => Err(TypeError::InvalidTimestamp),
                LocalResult::Single(v) => Ok(Some(v.naive_utc().date())),
                LocalResult::Ambiguous(_, _) => Err(TypeError::AmbiguousTimestamp),
            },
            _ => Ok(None),
        }
    }

    pub fn to_bson(&self) -> Option<&[u8]> {
        match self {
            Field::Bson(b) => Some(b),
            _ => None,
        }
    }

    pub fn to_point(&self) -> Option<&DozerPoint> {
        match self {
            Field::Point(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_duration(&self) -> Option<&DozerDuration> {
        match self {
            Field::Duration(d) => Some(d),
            _ => None,
        }
    }

    pub fn to_null(&self) -> Option<()> {
        match self {
            Field::Null => Some(()),
            _ => None,
        }
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::UInt(v) => f.write_str(&format!("{v} (64-bit unsigned int)")),
            Field::U128(v) => f.write_str(&format!("{v} (128-bit unsigned int)")),
            Field::Int(v) => f.write_str(&format!("{v} (64-bit signed int)")),
            Field::I128(v) => f.write_str(&format!("{v} (128-bit signed int)")),
            Field::Float(v) => f.write_str(&format!("{v} (Float)")),
            Field::Decimal(v) => f.write_str(&format!("{v} (Decimal)")),
            Field::Boolean(v) => f.write_str(&format!("{v}")),
            Field::String(v) => f.write_str(&v.to_string()),
            Field::Text(v) => f.write_str(&v.to_string()),
            Field::Binary(v) => f.write_str(&format!("{v:x?}")),
            Field::Timestamp(v) => f.write_str(&format!("{v}")),
            Field::Date(v) => f.write_str(&format!("{v}")),
            Field::Bson(v) => f.write_str(&format!("{v:x?}")),
            Field::Point(v) => f.write_str(&format!("{v} (Point)")),
            Field::Duration(d) => f.write_str(&format!("{:?} {:?} (Duration)", d.0, d.1)),
            Field::Null => f.write_str("NULL"),
        }
    }
}

impl<'a> FieldBorrow<'a> {
    pub fn to_owned(self) -> Field {
        match self {
            FieldBorrow::UInt(i) => Field::UInt(i),
            FieldBorrow::U128(i) => Field::U128(i),
            FieldBorrow::Int(i) => Field::Int(i),
            FieldBorrow::I128(i) => Field::I128(i),
            FieldBorrow::Float(f) => Field::Float(f),
            FieldBorrow::Decimal(d) => Field::Decimal(d),
            FieldBorrow::Boolean(b) => Field::Boolean(b),
            FieldBorrow::String(s) => Field::String(s.to_owned()),
            FieldBorrow::Text(s) => Field::Text(s.to_owned()),
            FieldBorrow::Binary(b) => Field::Binary(b.to_owned()),
            FieldBorrow::Timestamp(t) => Field::Timestamp(t),
            FieldBorrow::Date(d) => Field::Date(d),
            FieldBorrow::Bson(b) => Field::Bson(b.to_owned()),
            FieldBorrow::Point(p) => Field::Point(p),
            FieldBorrow::Duration(d) => Field::Duration(DozerDuration(d.0, d.1)),
            FieldBorrow::Null => Field::Null,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// All field types supported in Dozer.
pub enum FieldType {
    /// Unsigned 64-bit integer.
    UInt,
    /// Unsigned 128-bit integer.
    U128,
    /// Signed 64-bit integer.
    Int,
    /// Signed 128-bit integer.
    I128,
    /// 64-bit floating point number.
    Float,
    /// `true` or `false`.
    Boolean,
    /// A string with limited length.
    String,
    /// A long string.
    Text,
    /// Raw bytes.
    Binary,
    /// `Decimal` represents a 128 bit representation of a fixed-precision decimal number.
    /// The finite set of values of type `Decimal` are of the form m / 10<sup>e</sup>,
    /// where m is an integer such that -2<sup>96</sup> < m < 2<sup>96</sup>, and e is an integer
    /// between 0 and 28 inclusive.
    Decimal,
    /// Timestamp up to nanoseconds.
    Timestamp,
    /// Allows for every date from Jan 1, 262145 BCE to Dec 31, 262143 CE.
    Date,
    /// Bytes that are valid BSON.
    Bson,
    /// A geographic point.
    Point,
    /// Duration up to nanoseconds.
    Duration,
}

impl TryFrom<&str> for FieldType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let res = match value.to_lowercase().as_str() {
            "uint" => FieldType::UInt,
            "u128" => FieldType::U128,
            "int" => FieldType::Int,
            "i128" => FieldType::I128,
            "float" => FieldType::Float,
            "decimal" => FieldType::Decimal,
            "boolean" => FieldType::Boolean,
            "string" => FieldType::String,
            "text" => FieldType::Text,
            "binary" => FieldType::Binary,
            "timestamp" => FieldType::Timestamp,
            "date" => FieldType::Date,
            "bson" => FieldType::Bson,
            "point" => FieldType::Point,
            "duration" => FieldType::Duration,
            _ => return Err(format!("Unsupported '{value}' type")),
        };

        Ok(res)
    }
}

impl Display for FieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::UInt => f.write_str("64-bit unsigned int"),
            FieldType::U128 => f.write_str("128-bit unsigned int"),
            FieldType::Int => f.write_str("64-bit int"),
            FieldType::I128 => f.write_str("128-bit int"),
            FieldType::Float => f.write_str("float"),
            FieldType::Boolean => f.write_str("boolean"),
            FieldType::String => f.write_str("string"),
            FieldType::Text => f.write_str("text"),
            FieldType::Binary => f.write_str("binary"),
            FieldType::Decimal => f.write_str("decimal"),
            FieldType::Timestamp => f.write_str("timestamp"),
            FieldType::Date => f.write_str("date"),
            FieldType::Bson => f.write_str("bson"),
            FieldType::Point => f.write_str("point"),
            FieldType::Duration => f.write_str("duration"),
        }
    }
}

/// Can't put it in `tests` module because of <https://github.com/rust-lang/cargo/issues/8379>
/// and we need this function in `dozer-cache`.
pub fn field_test_cases() -> impl Iterator<Item = Field> {
    [
        Field::UInt(0_u64),
        Field::UInt(1_u64),
        Field::U128(0_u128),
        Field::U128(1_u128),
        Field::Int(0_i64),
        Field::Int(1_i64),
        Field::I128(0_i128),
        Field::I128(1_i128),
        Field::Float(OrderedFloat::from(0_f64)),
        Field::Float(OrderedFloat::from(1_f64)),
        Field::Decimal(Decimal::new(0, 0)),
        Field::Decimal(Decimal::new(1, 0)),
        Field::Boolean(true),
        Field::Boolean(false),
        Field::String("".to_string()),
        Field::String("1".to_string()),
        Field::Text("".to_string()),
        Field::Text("1".to_string()),
        Field::Binary(vec![]),
        Field::Binary(vec![1]),
        Field::Timestamp(DateTime::from(Utc.timestamp_millis_opt(0).unwrap())),
        Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap()),
        Field::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        Field::Date(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
        Field::Bson(vec![
            // BSON representation of `{"abc":"foo"}`
            123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125,
        ]),
        Field::Null,
    ]
    .into_iter()
}

#[cfg(feature = "python")]
impl pyo3::ToPyObject for Field {
    fn to_object(&self, py: pyo3::Python<'_>) -> pyo3::PyObject {
        match self {
            Field::UInt(val) => val.to_object(py),
            Field::U128(val) => val.to_object(py),
            Field::Int(val) => val.to_object(py),
            Field::I128(val) => val.to_object(py),
            Field::Float(val) => val.0.to_object(py),
            Field::Decimal(val) => val.to_f64().unwrap().to_object(py),
            Field::Boolean(val) => val.to_object(py),
            Field::String(val) => val.to_object(py),
            Field::Text(val) => val.to_object(py),
            Field::Binary(val) => val.to_object(py),
            Field::Timestamp(val) => val.timestamp().to_object(py),
            Field::Date(val) => {
                pyo3::types::PyDate::new(py, val.year(), val.month() as u8, val.day() as u8)
                    .unwrap()
                    .to_object(py)
            }
            Field::Bson(val) => val.to_object(py),
            Field::Point(_val) => todo!(),
            Field::Duration(_d) => todo!(),
            Field::Null => unreachable!(),
        }
    }
}
