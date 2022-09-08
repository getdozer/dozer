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








// pub struct Bool {
//     value: bool
// }
//
// impl Bool {
//     pub fn new(value: bool) -> Self {
//         Self { value }
//     }
// }
//
// impl BoolValue for Bool {}
//
// impl Value for Bool {
//     fn get_value(&self) -> Primitive {
//         return PrimitiveBoolean(self.value);
//     }
// }

// // Numeric
// pub trait NumericValue : Value {}
//
// // Integer
// pub trait IntValue : NumericValue {}
//
// pub struct Int {
//     value: i64
// }
//
// impl NumericValue for Int {}
// impl IntValue for Int {}
//
// impl Int {
//     pub fn new(value: i64) -> Self {
//         Self { value }
//     }
// }

// impl Value for Int {
//     fn get_value(&self) -> Primitive {
//         return PrimitiveInt(self.value);
//     }
// }


// Float
// pub trait FloatValue : NumericValue {}
//
// pub struct Float {
//     value: f64
// }
//
// impl FloatValue for Float {}
// impl NumericValue for Float {}
//
// impl Float {
//     pub fn new(value: f64) -> Self {
//         Self { value }
//     }
// }
//
// impl Value for Float {
//     fn get_value(&self) -> Primitive {
//         return PrimitiveFloat(self.value);
//     }
// }


// String
// pub trait StrValue {}
//
// pub struct Str {
//     value: String
// }
//
// impl StrValue for Str {}
//
// impl Str {
//     pub fn new(value: String) -> Self {
//         Self { value }
//     }
// }
//
// impl Value for Str {
//     fn get_value(&self) -> Primitive {
//         return PrimitiveString(self.value.clone());
//     }
// }

// Timestamp
// pub trait TimestampValue {}
//
// pub struct Timestamp {
//     value: u64
// }
//
// impl TimestampValue for Timestamp {}
//
// impl Timestamp {
//     pub fn new(value: u64) -> Self {
//         Self { value }
//     }
// }
//
// impl Value for Timestamp {
//     fn get_value(&self) -> Primitive {
//         return PrimitiveTimestamp(self.value);
//     }
// }














