use crate::types::Field;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;

fn field_test_cases() -> impl Iterator<Item = Field> {
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

#[test]
fn test_field_serialize_roundtrip() {
    for field in field_test_cases() {
        let bytes = field.to_bytes();
        let deserialized = Field::from_bytes(&bytes).unwrap();
        assert_eq!(field, deserialized);
    }
}

#[test]
fn field_serialization_should_never_be_empty() {
    for field in field_test_cases() {
        let bytes = field.to_bytes();
        assert!(!bytes.is_empty());
    }
}

#[test]
fn test_borrow_roundtrip() {
    for field in field_test_cases() {
        assert_eq!(field.borrow().to_owned(), field);
    }
}
