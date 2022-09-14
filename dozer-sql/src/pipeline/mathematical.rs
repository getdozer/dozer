use num_traits::cast::*;
use num_traits::Bounded;
use crate::pipeline::operator::Expression;
use dozer_shared::types::Field;
use dozer_shared::types::Field::{Invalid, Boolean};

macro_rules! define_math_oper {
    ($id:ident, $fct:expr, $t: expr) => {

        pub struct $id {
            left: Box <dyn Expression>,
            right: Box<dyn Expression>
        }

        impl $id {
            pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
                Self { left, right }
            }
        }


        impl Expression for $id {
            fn get_result(&self) -> Field {

                let left_p = self.left.get_result();
                let right_p = self.right.get_result();

                match left_p {
                    Field::Float(left_v) => {
                        match right_p {
                            Field::Int(right_v) => {
                                return Field::Float($fct(left_v,f64::from_i64(right_v).unwrap()));
                            }
                            Field::Float(right_v) => {
                                return Field::Float($fct(left_v,right_v));
                            }
                            _ => {
                                return Field::Invalid("Unable to perform a sum on non-numeric types".to_string());
                            }
                        }
                    }
                    Field::Int(left_v) => {
                        match right_p {
                            Field::Int(right_v) => {
                                return match ($t) {
                                    1 => { Field::Float($fct(f64::from_i64(left_v).unwrap(),f64::from_i64(right_v).unwrap())) }
                                    _ => { Field::Int($fct(left_v, right_v)) }
                                };
                            }
                            Field::Float(right_v) => {
                                return Field::Float($fct(f64::from_i64(left_v).unwrap(), right_v));
                            }
                            _ => {
                                return Field::Invalid("Unable to perform a sum on non-numeric types".to_string());
                            }
                        }
                    }
                    _ => {
                        return Field::Invalid("Unable to perform a sum on non-numeric types".to_string());
                    }
                }

            }
        }

    };
}

define_math_oper!(Add, |a,b| { a + b }, 0);
define_math_oper!(Sub, |a,b| { a - b }, 0);
define_math_oper!(Mul, |a,b| { a * b }, 0);
define_math_oper!(Div, |a,b| { a / b }, 1);
define_math_oper!(Mod, |a,b| { a % b }, 0);

#[test]
fn test_int_int_div() {
    let f0 = Box::new(1);
    let f1 = Box::new(2);
    let eq = Div::new(f0, f1);
    assert!(matches!(eq.get_result(), Field::Float(0.5)));
}


#[test]
fn test_float_int_sum() {
    let f0 = Box::new(1.3);
    let f1 = Box::new(1);
    let eq = Add::new(f0, f1);
    assert!(matches!(eq.get_result(), Field::Float(2.3)));
}

#[test]
fn test_int_int_sum() {
    let f0 = Box::new(1);
    let f1 = Box::new(1);
    let eq = Add::new(f0, f1);
    assert!(matches!(eq.get_result(), Field::Int(2)));
}

#[test]
fn test_int_float_sum() {
    let f0 = Box::new(1.3);
    let f1 = Box::new(1);
    let eq = Add::new(f0, f1);
    assert!(matches!(eq.get_result(), Field::Float(2.3)));
}


#[test]
fn test_composite_sum() {
    let f0 = Box::new(1);
    let f1 = Box::new(1);
    let f2 = Box::new(2.5);
    let eq = Add::new(f0, Box::new(Add::new(f1, f2)));
    assert!(matches!(eq.get_result(), Field::Float(4.5)));
}









