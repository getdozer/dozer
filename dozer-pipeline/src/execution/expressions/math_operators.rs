use num_traits::cast::*;
use num_traits::Bounded;
use crate::execution::expressions::values::{NumericValue, Value, ValueTypes};

pub struct Sum {
    left: Box <dyn NumericValue>,
    right: Box<dyn NumericValue>
}

impl Sum {
    pub fn new(left: Box<dyn NumericValue>, right: Box<dyn NumericValue>) -> Self {
        Self { left, right }
    }
}

impl NumericValue for Sum {}
impl Value for Sum {
    fn get_value(&self) -> ValueTypes {

        let left_p = self.left.get_value();
        let right_p = self.right.get_value();

        match left_p {
            ValueTypes::Float(left_v) => {
                match right_p {
                    ValueTypes::Int(right_v) => {
                        return ValueTypes::Float(left_v + f64::from_i64(right_v).unwrap());
                    }
                    ValueTypes::Float(right_v) => {
                        return ValueTypes::Float(left_v + right_v);
                    }
                    _ => {
                        return ValueTypes::Invalid("Unable to perform a sum on non-numeric types".to_string());
                    }
                }
            }
            ValueTypes::Int(left_v) => {
                match right_p {
                    ValueTypes::Int(right_v) => {
                        return ValueTypes::Int(right_v + left_v);
                    }
                    ValueTypes::Float(right_v) => {
                        return ValueTypes::Float(f64::from_i64(left_v).unwrap() + right_v);
                    }
                    _ => {
                        return ValueTypes::Invalid("Unable to perform a sum on non-numeric types".to_string());
                    }
                }
            }
            _ => {
                return ValueTypes::Invalid("Unable to perform a sum on non-numeric types".to_string());
            }
        }

    }
}


#[test]
fn test_float_int_sum() {
    let f0 = Box::new(1.3);
    let f1 = Box::new(1);
    let eq = Sum::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Float(2.3)));
}

#[test]
fn test_composite_sum() {
    let f0 = Box::new(1);
    let f1 = Box::new(1);
    let f2 = Box::new(2.5);
    let eq = Sum::new(f0, Box::new(Sum::new(f1, f2)));
    assert!(matches!(eq.get_value(), ValueTypes::Float(4.5)));
}







