use num_traits::cast::*;
use num_traits::Bounded;
use crate::execution::expressions::values::ValueTypes::{Boolean, Float, Int, Str, Ts};

pub enum ValueTypes {
    Int(i64),
    Float(f64),
    Boolean(bool),
    Str(String),
    Binary(Vec<u8>),
    Ts(u64),
    Invalid(String)
}


pub trait Value {
    fn get_value(&self) -> ValueTypes;
}

// Boolean
pub trait BoolValue : Value {}

impl BoolValue for bool {}
impl Value for bool {
    fn get_value(&self) -> ValueTypes {
        return Boolean(*self);
    }
}

// Numeric
pub trait NumericValue : Value {}

// Integer
pub trait IntValue : NumericValue {}

impl IntValue for i64 {}
impl NumericValue for i64 {}
impl Value for i64 {
    fn get_value(&self) -> ValueTypes {
        return Int(*self);
    }
}

impl IntValue for i32 {}
impl NumericValue for i32 {}
impl Value for i32 {
    fn get_value(&self) -> ValueTypes {
        return Int(i64::from_i32(*self).unwrap());
    }
}

// Float
pub trait FloatValue : NumericValue {}

impl FloatValue for f32 {}
impl NumericValue for f32 {}
impl Value for f32 {
    fn get_value(&self) -> ValueTypes {
        return Float(f64::from_f32(*self).unwrap());
    }
}

impl FloatValue for f64 {}
impl NumericValue for f64 {}
impl Value for f64 {
    fn get_value(&self) -> ValueTypes {
        return Float(*self);
    }
}

// String

pub trait StrValue : Value {}

impl StrValue for String {}
impl Value for String {
    fn get_value(&self) -> ValueTypes {
        return Str((*self).clone());
    }
}

// Timestamp
pub trait TimestampValue {}

pub struct Timestamp {
    value: u64
}

impl TimestampValue for Timestamp {}

impl Timestamp {
    pub fn new(value: u64) -> Self {
        Self { value }
    }
}

impl Value for Timestamp {
    fn get_value(&self) -> ValueTypes {
        return Ts(self.value);
    }
}












