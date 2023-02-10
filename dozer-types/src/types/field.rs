use crate::errors::types::{DeserializationError, TypeError};
use chrono::{DateTime, FixedOffset, LocalResult, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};

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
            8 => {
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
            9 => Ok(FieldBorrow::Date(NaiveDate::parse_from_str(
                std::str::from_utf8(val)?,
                DATE_FORMAT,
            )?)),
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

    pub fn as_null(&self) -> Option<()> {
        match self {
            Field::Null => Some(()),
            _ => None,
        }
    }

    pub fn to_uint(&self) -> Option<u64> {
        match self {
            Field::UInt(i) => Some(*i),
            Field::Int(i) => u64::from_i64(*i),
            Field::String(s) => s.parse::<u64>().ok(),
            Field::Null => Some(0_u64),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            Field::Int(i) => Some(*i),
            Field::UInt(u) => i64::from_u64(*u),
            Field::String(s) => s.parse::<i64>().ok(),
            Field::Null => Some(0_i64),
            _ => None,
        }
    }

    pub fn to_float(&self) -> Option<f64> {
        match self {
            Field::Float(f) => Some(f.0),
            Field::Decimal(d) => d.to_f64(),
            Field::UInt(u) => f64::from_u64(*u),
            Field::Int(i) => f64::from_i64(*i),
            Field::Null => Some(0_f64),
            Field::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn to_boolean(&self) -> Option<bool> {
        match self {
            Field::Boolean(b) => Some(*b),
            Field::Null => Some(false),
            Field::Int(i) => Some(*i > 0_i64),
            Field::UInt(i) => Some(*i > 0_u64),
            Field::Float(i) => Some(i.0 > 0_f64),
            Field::Decimal(i) => Some(i.gt(&Decimal::from(0_u64))),
            _ => None,
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            Field::String(s) => Some(s.to_owned()),
            Field::Text(t) => Some(t.to_owned()),
            Field::Int(i) => Some(format!("{i}")),
            Field::UInt(i) => Some(format!("{i}")),
            Field::Float(i) => Some(format!("{i}")),
            Field::Decimal(i) => Some(format!("{i}")),
            Field::Boolean(i) => Some(if *i {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }),
            Field::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Field::Timestamp(t) => Some(t.to_rfc3339()),
            Field::Binary(b) => Some(format!("{b:X?}")),
            Field::Null => Some("".to_string()),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<String> {
        match self {
            Field::String(s) => Some(s.to_owned()),
            Field::Text(t) => Some(t.to_owned()),
            Field::Int(i) => Some(format!("{i}")),
            Field::UInt(i) => Some(format!("{i}")),
            Field::Float(i) => Some(format!("{i}")),
            Field::Decimal(i) => Some(format!("{i}")),
            Field::Boolean(i) => Some(if *i {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }),
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
            Field::Decimal(d) => Some(*d),
            Field::Float(f) => Decimal::from_f64_retain(f.0),
            Field::Int(i) => Decimal::from_i64(*i),
            Field::UInt(u) => Decimal::from_u64(*u),
            Field::Null => Some(Decimal::from(0)),
            Field::String(s) => Decimal::from_str_exact(s).ok(),
            _ => None,
        }
    }

    pub fn to_timestamp(&self) -> Result<Option<DateTime<FixedOffset>>, TypeError> {
        match self {
            Field::Timestamp(t) => Ok(Some(*t)),
            Field::String(s) => Ok(DateTime::parse_from_rfc3339(s.as_str()).ok()),
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
            Field::Date(d) => Ok(Some(*d)),
            Field::String(s) => Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()),
            Field::Null => match Utc.timestamp_millis_opt(0) {
                LocalResult::None => Err(TypeError::InvalidTimestamp),
                LocalResult::Single(v) => Ok(Some(v.unwrap().naive_utc().date())),
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
            Field::UInt(v) => f.write_str(&format!("{v} (unsigned int)")),
            Field::Int(v) => f.write_str(&format!("{v} (signed int)")),
            Field::Float(v) => f.write_str(&format!("{v} (Float)")),
            Field::Boolean(v) => f.write_str(&format!("{v}")),
            Field::String(v) => f.write_str(&v.to_string()),
            Field::Text(v) => f.write_str(&v.to_string()),
            Field::Binary(v) => f.write_str(&format!("{v:x?}")),
            Field::Decimal(v) => f.write_str(&format!("{v} (Decimal)")),
            Field::Timestamp(v) => f.write_str(&format!("{v}")),
            Field::Date(v) => f.write_str(&format!("{v}")),
            Field::Bson(v) => f.write_str(&format!("{v:x?}")),
            Field::Null => f.write_str("NULL"),
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

impl Display for FieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::UInt => f.write_str("unsigned int"),
            FieldType::Int => f.write_str("int"),
            FieldType::Float => f.write_str("float"),
            FieldType::Boolean => f.write_str("boolean"),
            FieldType::String => f.write_str("string"),
            FieldType::Text => f.write_str("text"),
            FieldType::Binary => f.write_str("binary"),
            FieldType::Decimal => f.write_str("decimal"),
            FieldType::Timestamp => f.write_str("timestamp"),
            FieldType::Date => f.write_str("date"),
            FieldType::Bson => f.write_str("bson"),
        }
    }
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

    #[test]
    fn test_as_conversion() {
        let field = Field::UInt(1);
        assert!(field.as_uint().is_some());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Int(1);
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_some());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Float(OrderedFloat::from(1.0));
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_some());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Boolean(true);
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_some());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::String("".to_string());
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_some());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Text("".to_string());
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_some());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Binary(vec![]);
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_some());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Decimal(Decimal::from(1));
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_some());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Timestamp(DateTime::from(Utc.timestamp_millis_opt(0).unwrap()));
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_some());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_some());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_none());

        let field = Field::Bson(vec![]);
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_some());
        assert!(field.as_null().is_none());

        let field = Field::Null;
        assert!(field.as_uint().is_none());
        assert!(field.as_int().is_none());
        assert!(field.as_float().is_none());
        assert!(field.as_boolean().is_none());
        assert!(field.as_string().is_none());
        assert!(field.as_text().is_none());
        assert!(field.as_binary().is_none());
        assert!(field.as_decimal().is_none());
        assert!(field.as_timestamp().is_none());
        assert!(field.as_date().is_none());
        assert!(field.as_bson().is_none());
        assert!(field.as_null().is_some());
    }

    #[test]
    fn test_to_conversion() {
        let field = Field::UInt(1);
        assert!(field.to_uint().is_some());
        assert!(field.to_int().is_some());
        assert!(field.to_float().is_some());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_some());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Int(1);
        assert!(field.to_uint().is_some());
        assert!(field.to_int().is_some());
        assert!(field.to_float().is_some());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_some());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Float(OrderedFloat::from(1.0));
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_some());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_some());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Boolean(true);
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::String("".to_string());
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Text("".to_string());
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Binary(vec![]);
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_some());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Decimal(Decimal::from(1));
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_some());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_some());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Timestamp(DateTime::from(Utc.timestamp_millis_opt(0).unwrap()));
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_some());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_some());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_none());

        let field = Field::Bson(vec![]);
        assert!(field.to_uint().is_none());
        assert!(field.to_int().is_none());
        assert!(field.to_float().is_none());
        assert!(field.to_boolean().is_none());
        assert!(field.to_string().is_none());
        assert!(field.to_text().is_none());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_none());
        assert!(field.to_timestamp().unwrap().is_none());
        assert!(field.to_date().unwrap().is_none());
        assert!(field.to_bson().is_some());
        assert!(field.to_null().is_none());

        let field = Field::Null;
        assert!(field.to_uint().is_some());
        assert!(field.to_int().is_some());
        assert!(field.to_float().is_some());
        assert!(field.to_boolean().is_some());
        assert!(field.to_string().is_some());
        assert!(field.to_text().is_some());
        assert!(field.to_binary().is_none());
        assert!(field.to_decimal().is_some());
        assert!(field.to_timestamp().unwrap().is_some());
        assert!(field.to_date().unwrap().is_some());
        assert!(field.to_bson().is_none());
        assert!(field.to_null().is_some());
    }
}
