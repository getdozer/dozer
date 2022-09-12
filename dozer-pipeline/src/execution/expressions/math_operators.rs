use num_traits::cast::*;
use num_traits::Bounded;
use crate::execution::expressions::values::{NumericValue, Value, ValueTypes};

macro_rules! define_math_oper {
    ($id:ident, $fct:expr, $t: expr) => {

        pub struct $id {
            left: Box <dyn Value>,
            right: Box<dyn Value>
        }

        impl $id {
            pub fn new(left: Box<dyn Value>, right: Box<dyn Value>) -> Self {
                Self { left, right }
            }
        }

        impl NumericValue for $id {}
        impl Value for $id {
            fn get_value(&self) -> ValueTypes {

                let left_p = self.left.get_value();
                let right_p = self.right.get_value();

                match left_p {
                    ValueTypes::Float(left_v) => {
                        match right_p {
                            ValueTypes::Int(right_v) => {
                                return ValueTypes::Float($fct(left_v,f64::from_i64(right_v).unwrap()));
                            }
                            ValueTypes::Float(right_v) => {
                                return ValueTypes::Float($fct(left_v,right_v));
                            }
                            _ => {
                                return ValueTypes::Invalid("Unable to perform a sum on non-numeric types".to_string());
                            }
                        }
                    }
                    ValueTypes::Int(left_v) => {
                        match right_p {
                            ValueTypes::Int(right_v) => {
                                return match ($t) {
                                    1 => { ValueTypes::Float($fct(f64::from_i64(left_v).unwrap(),f64::from_i64(right_v).unwrap())) }
                                    _ => { ValueTypes::Int($fct(left_v, right_v)) }
                                };
                            }
                            ValueTypes::Float(right_v) => {
                                return ValueTypes::Float($fct(f64::from_i64(left_v).unwrap(), right_v));
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

    };
}

define_math_oper!(Sum, |a,b| { a + b }, 0);
define_math_oper!(Diff, |a,b| { a - b }, 0);
define_math_oper!(Mult, |a,b| { a * b }, 0);
define_math_oper!(Div, |a,b| { a / b }, 1);
define_math_oper!(Mod, |a,b| { a % b }, 0);

#[test]
fn test_int_int_div() {
    let f0 = Box::new(1);
    let f1 = Box::new(2);
    let eq = Div::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Float(0.5)));
}


#[test]
fn test_float_int_sum() {
    let f0 = Box::new(1.3);
    let f1 = Box::new(1);
    let eq = Sum::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Float(2.3)));
}

#[test]
fn test_int_int_sum() {
    let f0 = Box::new(1);
    let f1 = Box::new(1);
    let eq = Sum::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Int(2)));
}

#[test]
fn test_int_float_sum() {
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









