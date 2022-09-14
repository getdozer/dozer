use crate::common::error::DozerSqlError;
use dozer_shared::types::Field;
use num_traits::FromPrimitive;
use chrono::{DateTime, Utc};

pub enum Operator {
    BinaryOperator {
        left: Box<dyn Expression>,
        op: BinaryOperatorType,
        right: Box<dyn Expression>,
    },
    UnaryOperator {
        left: Box<dyn Expression>,
        op: UnaryOperatorType,
    },
}

pub enum BinaryOperatorType {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,

    Sum,
    Dif,
    Mul,
    Div,
    Mod,

    And,
    Or,
}

pub enum UnaryOperatorType {
    Not,
}

pub trait Expression {
    fn get_result(&self) -> Field;
}

impl Expression for bool {
    fn get_result(&self) -> Field {
        return Field::Boolean(*self);
    }
}

impl Expression for i32 {
    fn get_result(&self) -> Field {
        return Field::Int(i64::from_i32(*self).unwrap());
    }
}

impl Expression for i64 {
    fn get_result(&self) -> Field {
        return Field::Int(*self);
    }
}

impl Expression for f32 {
    fn get_result(&self) -> Field {
        return Field::Float(f64::from_f32(*self).unwrap());
    }
}

impl Expression for f64 {
    fn get_result(&self) -> Field {
        return Field::Float(*self);
    }
}

impl Expression for String {
    fn get_result(&self) -> Field {
        return Field::String((*self).clone());
    }
}

pub struct Timestamp {
    value: DateTime<Utc>
}

impl Timestamp {
    pub fn new(value: DateTime<Utc>) -> Self {
        Self { value }
    }
}

impl Expression for Timestamp {
    fn get_result(&self) -> Field {
        return Field::Timestamp(self.value);
    }
}

pub struct Column {
    name: String,
}

impl Column {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Expression for Column {
    fn get_result(&self) -> Field {
        return Field::Int(0);
    }
}

#[test]
fn test_bool() {
    let v = true;
    assert!(matches!(v.get_result(), Field::Boolean(true)));
}

#[test]
fn test_i32() {
    let v = i32::MAX;
    let c = Field::Int(i64::from_i32(i32::MAX).unwrap());
    assert!(matches!(v.get_result(), c));
}

#[test]
fn test_i64() {
    let v = i64::MAX;
    assert!(matches!(v.get_result(), Field::Int(i64::MAX)));
}

#[test]
fn test_f32() {
    let v = f32::MAX;
    let c = Field::Float(f64::from_f32(f32::MAX).unwrap());
    assert!(matches!(v.get_result(), c));
}

#[test]
fn test_f64() {
    let v = f64::MAX;
    assert!(matches!(v.get_result(), Field::Float(f64::MAX)));
}

#[test]
fn test_string() {
    let v = "Hello".to_string();
    let c = Field::String("Hello".to_string());
    assert!(matches!(v.get_result(), c));
}

use chrono::NaiveDateTime;

#[test]
fn test_timestamp() {
    let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(61, 0), Utc);
    let v = Timestamp::new(time);
    assert!(matches!(v.get_result(), Field::Timestamp(time)));
}

#[test]
fn test_column() {
    let v = Column::new("column1".to_string());
    assert!(matches!(v.get_result(), Field::Int(0)));
}
