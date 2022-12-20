use crate::errors::types::DeserializationError;
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};
use std::borrow::Cow;

pub const DATE_FORMAT: &str = "%Y-%m-%d";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Field {
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    String(String),
    Text(String),
    Binary(Vec<u8>),
    #[serde(with = "rust_decimal::serde::float")]
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Bson(Vec<u8>),
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum FieldBorrow<'a> {
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    String(&'a str),
    Text(&'a str),
    Binary(&'a [u8]),
    #[serde(with = "rust_decimal::serde::float")]
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Bson(&'a [u8]),
    Null,
}

impl Field {
    fn data_encoding_len(&self) -> usize {
        match self {
            Field::UInt(_) => 8,
            Field::Int(_) => 8,
            Field::Float(_) => 8,
            Field::Boolean(_) => 1,
            Field::String(s) => s.len(),
            Field::Text(s) => s.len(),
            Field::Binary(b) => b.len(),
            Field::Decimal(_) => 16,
            Field::Timestamp(_) => 8,
            Field::Date(_) => 10,
            Field::Bson(b) => b.len(),
            Field::Null => 0,
        }
    }

    fn encode_data(&self) -> Cow<[u8]> {
        match self {
            Field::UInt(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::Int(i) => Cow::Owned(i.to_be_bytes().into()),
            Field::Float(f) => Cow::Owned(f.to_be_bytes().into()),
            Field::Boolean(b) => Cow::Owned(if *b { [1] } else { [0] }.into()),
            Field::String(s) => Cow::Borrowed(s.as_bytes()),
            Field::Text(s) => Cow::Borrowed(s.as_bytes()),
            Field::Binary(b) => Cow::Borrowed(b.as_slice()),
            Field::Decimal(d) => Cow::Owned(d.serialize().into()),
            Field::Timestamp(t) => Cow::Owned(t.timestamp_millis().to_be_bytes().into()),
            Field::Date(t) => Cow::Owned(t.to_string().into()),
            Field::Bson(b) => Cow::Borrowed(b),
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
            Field::Int(i) => FieldBorrow::Int(*i),
            Field::Float(f) => FieldBorrow::Float(*f),
            Field::Boolean(b) => FieldBorrow::Boolean(*b),
            Field::String(s) => FieldBorrow::String(s),
            Field::Text(s) => FieldBorrow::Text(s),
            Field::Binary(b) => FieldBorrow::Binary(b),
            Field::Decimal(d) => FieldBorrow::Decimal(*d),
            Field::Timestamp(t) => FieldBorrow::Timestamp(*t),
            Field::Date(t) => FieldBorrow::Date(*t),
            Field::Bson(b) => FieldBorrow::Bson(b),
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
            1 => Ok(FieldBorrow::Int(i64::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            2 => Ok(FieldBorrow::Float(OrderedFloat(f64::from_be_bytes(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            )))),
            3 => Ok(FieldBorrow::Boolean(val[0] == 1)),
            4 => Ok(FieldBorrow::String(std::str::from_utf8(val)?)),
            5 => Ok(FieldBorrow::Text(std::str::from_utf8(val)?)),
            6 => Ok(FieldBorrow::Binary(val)),
            7 => Ok(FieldBorrow::Decimal(Decimal::deserialize(
                val.try_into()
                    .map_err(|_| DeserializationError::BadDataLength)?,
            ))),
            8 => Ok(FieldBorrow::Timestamp(DateTime::from(
                Utc.timestamp_millis(i64::from_be_bytes(
                    val.try_into()
                        .map_err(|_| DeserializationError::BadDataLength)?,
                )),
            ))),
            9 => Ok(FieldBorrow::Date(
                NaiveDate::parse_from_str(std::str::from_utf8(val)?, DATE_FORMAT).unwrap(),
            )),
            10 => Ok(FieldBorrow::Bson(val)),
            11 => Ok(FieldBorrow::Null),
            other => Err(DeserializationError::UnrecognisedFieldType(other)),
        }
    }

    fn get_type_prefix(&self) -> u8 {
        match self {
            Field::UInt(_) => 0,
            Field::Int(_) => 1,
            Field::Float(_) => 2,
            Field::Boolean(_) => 3,
            Field::String(_) => 4,
            Field::Text(_) => 5,
            Field::Binary(_) => 6,
            Field::Decimal(_) => 7,
            Field::Timestamp(_) => 8,
            Field::Date(_) => 9,
            Field::Bson(_) => 10,
            Field::Null => 11,
        }
    }

    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Field::UInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Field::Int(i) => Some(*i),
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

    pub fn as_decimal(&self) -> Option<&Decimal> {
        match self {
            Field::Decimal(d) => Some(d),
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

    pub fn as_null(&self) -> Option<()> {
        match self {
            Field::Null => Some(()),
            _ => None,
        }
    }
}

impl<'a> FieldBorrow<'a> {
    pub fn to_owned(self) -> Field {
        match self {
            FieldBorrow::Int(i) => Field::Int(i),
            FieldBorrow::UInt(i) => Field::UInt(i),
            FieldBorrow::Float(f) => Field::Float(f),
            FieldBorrow::Boolean(b) => Field::Boolean(b),
            FieldBorrow::String(s) => Field::String(s.to_owned()),
            FieldBorrow::Text(s) => Field::Text(s.to_owned()),
            FieldBorrow::Binary(b) => Field::Binary(b.to_owned()),
            FieldBorrow::Decimal(d) => Field::Decimal(d),
            FieldBorrow::Timestamp(t) => Field::Timestamp(t),
            FieldBorrow::Date(d) => Field::Date(d),
            FieldBorrow::Bson(b) => Field::Bson(b.to_owned()),
            FieldBorrow::Null => Field::Null,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum FieldType {
    UInt,
    Int,
    Float,
    Boolean,
    String,
    Text,
    Binary,
    Decimal,
    Timestamp,
    Date,
    Bson,
}

/// Can't put it in `tests` module because of <https://github.com/rust-lang/cargo/issues/8379>
/// and we need this function in `dozer-cache`.
pub fn field_test_cases() -> impl Iterator<Item = Field> {
    [
        Field::Int(0_i64),
        Field::Int(1_i64),
        Field::UInt(0_u64),
        Field::UInt(1_u64),
        Field::Float(OrderedFloat::from(0_f64)),
        Field::Float(OrderedFloat::from(1_f64)),
        Field::Boolean(true),
        Field::Boolean(false),
        Field::String("".to_string()),
        Field::String("1".to_string()),
        Field::Text("".to_string()),
        Field::Text("1".to_string()),
        Field::Binary(vec![]),
        Field::Binary(vec![1]),
        Field::Decimal(Decimal::new(0, 0)),
        Field::Decimal(Decimal::new(1, 0)),
        Field::Timestamp(DateTime::from(Utc.timestamp_millis(0))),
        Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap()),
        Field::Date(NaiveDate::from_ymd(1970, 1, 1)),
        Field::Date(NaiveDate::from_ymd(2020, 1, 1)),
        Field::Bson(vec![
            // BSON representation of `{"abc":"foo"}`
            123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125,
        ]),
        Field::Null,
    ]
    .into_iter()
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn data_encoding_len_must_agree_with_encode() {
        for field in field_test_cases() {
            let bytes = field.encode_data();
            assert_eq!(bytes.len(), field.data_encoding_len());
        }
    }
}
