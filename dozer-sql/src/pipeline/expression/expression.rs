use crate::common::error::DozerSqlError;
use chrono::{DateTime, NaiveDateTime, Utc};
use dozer_types::types::{Field, Record};
use dozer_types::types::Field::{Boolean, Invalid};
use num_traits::FromPrimitive;
use sqlparser::ast::{BinaryOperator, DateTimeField};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::scalar::ScalarFunctionType;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};

#[derive(Clone, PartialEq)]
pub enum Expression {
    Column {
        index: usize
    },
    Literal(Field),
    UnaryOperator {
        operator: UnaryOperatorType,
        arg: Box<Expression>,
    },
    BinaryOperator {
        left: Box<Expression>,
        operator: BinaryOperatorType,
        right: Box<Expression>,
    },
    ScalarFunction {
        fun: ScalarFunctionType,
        args: Vec<Box<Expression>>,
    },
    AggregateFunction {
        fun: AggregateFunctionType,
        args: Vec<Box<Expression>>,
    }
}


pub trait PhysicalExpression: Send + Sync {
    fn evaluate(&self, record: &Record) -> Field;
}

impl PhysicalExpression for Expression {
    fn evaluate(&self, record: &Record) -> Field {
        match self {
            Expression::Column{index} => record.values.get(*index).unwrap().clone(),
            Expression::BinaryOperator {left, operator, right} => operator.evaluate(left, right, record),
            Expression::ScalarFunction{fun, args} => fun.evaluate( args, record),
            _ => Field::Int(99)
        }
    }
}

impl PhysicalExpression for bool {
    fn evaluate(&self, record: &Record) -> Field {
        return Field::Boolean(*self);
    }
}

impl PhysicalExpression for i32 {
    fn evaluate(&self, record: &Record) -> Field {
        return Field::Int(i64::from_i32(*self).unwrap());
    }
}

impl PhysicalExpression for i64 {
    fn evaluate(&self, record: &Record) -> Field {
        return Field::Int(*self);
    }
}

impl PhysicalExpression for f32 {
    fn evaluate(&self, record: &Record) -> Field {
        return Field::Float(f64::from_f32(*self).unwrap());
    }
}

impl PhysicalExpression for f64 {
    fn evaluate(&self, record: &Record) -> Field {
        return Field::Float(*self);
    }
}

impl PhysicalExpression for String {
    fn evaluate(&self, record: &Record) -> Field {
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

impl PhysicalExpression for Timestamp {
    fn evaluate(&self, record: &Record) -> Field {
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

impl PhysicalExpression for Column {
    fn evaluate(&self, record: &Record) -> Field {
        record.values.get(self.index).unwrap().clone()
    }
}

#[test]
fn test_bool() {
    let row = Record::new(None, vec![]);
    let v = true;
    assert!(matches!(v.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_i32() {
    let row = Record::new(None, vec![]);
    let v = i32::MAX;
    let c = Field::Int(i64::from_i32(i32::MAX).unwrap());
    assert!(matches!(v.evaluate(&row), c));
}

#[test]
fn test_i64() {
    let row = Record::new(None, vec![]);
    let v = i64::MAX;
    assert!(matches!(v.evaluate(&row), Field::Int(i64::MAX)));
}

#[test]
fn test_f32() {
    let row = Record::new(None, vec![]);
    let v = f32::MAX;
    let c = Field::Float(f64::from_f32(f32::MAX).unwrap());
    assert!(matches!(v.evaluate(&row), c));
}

#[test]
fn test_f64() {
    let row = Record::new(None, vec![]);
    let v = f64::MAX;
    assert!(matches!(v.evaluate(&row), Field::Float(f64::MAX)));
}

#[test]
fn test_string() {
    let row = Record::new(None, vec![]);
    let v = "Hello".to_string();
    let c = Field::String("Hello".to_string());
    assert!(matches!(v.evaluate(&row), c));
}

#[test]
fn test_timestamp() {
    let row = Record::new(None, vec![]);
    let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(61, 0), Utc);
    let v = Timestamp::new(time);
    assert!(matches!(v.evaluate(&row), Field::Timestamp(time)));
}

#[test]
fn test_column() {
    let row = Record::new(None, vec![Field::Int(101), Field::Float(3.1337)]);
    let v = Column::new(1);
    assert!(matches!(v.evaluate(&row), Field::Float(3.1337)));
}
