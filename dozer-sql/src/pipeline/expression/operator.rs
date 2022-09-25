use crate::common::error::DozerSqlError;
use chrono::{DateTime, NaiveDateTime, Utc};
use dozer_types::types::{Field, Record};
use num_traits::FromPrimitive;
use sqlparser::ast::BinaryOperator;

pub trait Expression: Send + Sync {
    fn get_result(&self, record: &Record) -> Field;
}

impl Expression for bool {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Boolean(*self);
    }
}

impl Expression for i32 {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Int(i64::from_i32(*self).unwrap());
    }
}

impl Expression for i64 {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Int(*self);
    }
}

impl Expression for f32 {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Float(f64::from_f32(*self).unwrap());
    }
}

impl Expression for f64 {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Float(*self);
    }
}

impl Expression for String {
    fn get_result(&self, record: &Record) -> Field {
        return Field::String((*self).clone());
    }
}

pub struct Timestamp {
    value: DateTime<Utc>,
}

impl Timestamp {
    pub fn new(value: DateTime<Utc>) -> Self {
        Self { value }
    }
}

impl Expression for Timestamp {
    fn get_result(&self, record: &Record) -> Field {
        return Field::Timestamp(self.value);
    }
}

pub struct Column {
    index: usize,
}

impl Column {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl Expression for Column {
    fn get_result(&self, record: &Record) -> Field {
        record.values.get(self.index).unwrap().clone()
    }
}

#[test]
fn test_bool() {
    let row = Record::new(0, vec![]);
    let v = true;
    assert!(matches!(v.get_result(&row), Field::Boolean(true)));
}

#[test]
fn test_i32() {
    let row = Record::new(0, vec![]);
    let v = i32::MAX;
    let c = Field::Int(i64::from_i32(i32::MAX).unwrap());
    assert!(matches!(v.get_result(&row), c));
}

#[test]
fn test_i64() {
    let row = Record::new(0, vec![]);
    let v = i64::MAX;
    assert!(matches!(v.get_result(&row), Field::Int(i64::MAX)));
}

#[test]
fn test_f32() {
    let row = Record::new(0, vec![]);
    let v = f32::MAX;
    let c = Field::Float(f64::from_f32(f32::MAX).unwrap());
    assert!(matches!(v.get_result(&row), c));
}

#[test]
fn test_f64() {
    let row = Record::new(0, vec![]);
    let v = f64::MAX;
    assert!(matches!(v.get_result(&row), Field::Float(f64::MAX)));
}

#[test]
fn test_string() {
    let row = Record::new(0, vec![]);
    let v = "Hello".to_string();
    let c = Field::String("Hello".to_string());
    assert!(matches!(v.get_result(&row), c));
}

#[test]
fn test_timestamp() {
    let row = Record::new(0, vec![]);
    let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(61, 0), Utc);
    let v = Timestamp::new(time);
    assert!(matches!(v.get_result(&row), Field::Timestamp(time)));
}

#[test]
fn test_column() {
    let row = Record::new(0, vec![Field::Int(101), Field::Float(3.1337)]);
    let v = Column::new(1);
    assert!(matches!(v.get_result(&row), Field::Float(3.1337)));
}
