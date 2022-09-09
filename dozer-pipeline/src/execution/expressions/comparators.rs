use num_traits::cast::*;
use num_traits::Bounded;
use crate::execution::expressions::math_operators::Mult;
use crate::execution::expressions::values::{BoolValue, ValueTypes, Timestamp, Value};
use crate::execution::expressions::values::ValueTypes::{Invalid, Boolean};


macro_rules! define_cmp_oper {
    ($id:ident, $fct:expr) => {
        pub struct $id {
            left: Box<dyn Value>,
            right: Box<dyn Value>
        }

        impl $id {
            pub fn new(left: Box<dyn Value>, right: Box<dyn Value>) -> Self {
                Self { left, right }
            }
        }

        impl BoolValue for $id {}

        impl Value for $id {

            fn get_value(&self) -> ValueTypes {

                let left_p = self.left.get_value();
                let right_p = self.right.get_value();

                match left_p {
                    ValueTypes::Boolean(left_v) => {
                        match right_p {
                            Boolean(right_v) => {
                                Boolean($fct(left_v,right_v))
                            }
                            _ => { Boolean(false) }
                        }
                    }
                    ValueTypes::Int(left_v) => {
                      match right_p {
                          ValueTypes::Int(right_v) => {
                              Boolean($fct(left_v,right_v))
                          }
                          ValueTypes::Float(right_v) => {
                              let left_v_f = f64::from_i64(left_v).unwrap();
                              Boolean($fct(left_v_f,right_v))
                          }
                          _ => {
                              return Invalid(format!("Cannot compare int value {} to the current value", left_v));
                          }

                      }

                    }
                    ValueTypes::Float(left_v) => {
                        match right_p {
                            ValueTypes::Float(right_v) => {
                                Boolean($fct(left_v,right_v))
                            }
                            ValueTypes::Int(right_v) => {
                                let right_v_f = f64::from_i64(right_v).unwrap();
                                Boolean($fct(left_v,right_v_f))
                            }
                            _ => {
                                return Invalid(format!("Cannot compare float value {} to the current value", left_v));
                            }
                        }
                    }
                    ValueTypes::Str(left_v) => {
                        match right_p {
                            ValueTypes::Str(right_v) => {
                                Boolean($fct(left_v,right_v))
                            }
                            _ => {
                                return Invalid(format!("Cannot compare string value {} to the current value", left_v));
                            }
                        }

                    }
                    ValueTypes::Ts(left_v) => {
                        match right_p {
                            ValueTypes::Ts(right_v) => {
                                Boolean($fct(left_v,right_v))
                            }
                            _ => {
                                return Invalid(format!("Cannot compare timestamp value {} to the current value", left_v));
                            }
                        }

                    }
                    ValueTypes::Binary(left_v) => {
                        return return Invalid(format!("Cannot compare binary value to the current value"));
                    }
                    ValueTypes::Invalid(cause) => {
                        return Invalid(cause);
                    }

                }

            }
        }
    }
}

define_cmp_oper!(Eq, |l,r| { l == r});
define_cmp_oper!(Ne, |l,r| { l != r});
define_cmp_oper!(Lt, |l,r| { l < r});
define_cmp_oper!(Lte, |l,r| { l <= r});
define_cmp_oper!(Gt, |l,r| { l > r});
define_cmp_oper!(Gte, |l,r| { l >= r});


#[test]
fn test_float_float_eq() {
    let f0 = Box::new(1.3);
    let f1 = Box::new(1.3);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}

#[test]
fn test_float_int_eq() {
    let f0 = Box::new(1.0);
    let f1 = Box::new(1);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}

#[test]
fn test_int_float_eq() {
    let f0 = Box::new(1);
    let f1 = Box::new(1.0);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}

#[test]
fn test_bool_bool_eq() {
    let f0 = Box::new(false);
    let f1 = Box::new(false);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}

#[test]
fn test_str_str_eq() {
    let f0 = Box::new("abc".to_string());
    let f1 = Box::new("abc".to_string());
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}

#[test]
fn test_ts_ts_eq() {
    let f0 = Box::new(Timestamp::new(1));
    let f1 = Box::new(Timestamp::new(1));
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.get_value(), ValueTypes::Boolean(true)));
}



