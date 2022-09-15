use num_traits::cast::*;
use num_traits::Bounded;
use crate::pipeline::expression::operator::{Timestamp, Expression};
use dozer_shared::types::Field;
use dozer_shared::types::Field::{Invalid, Boolean};

macro_rules! define_cmp_oper {
    ($id:ident, $fct:expr) => {
        pub struct $id {
            left: Box<dyn Expression>,
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
                    Field::Boolean(left_v) => {
                        match right_p {
                            Field::Boolean(right_v) => {
                                Field::Boolean($fct(left_v,right_v))
                            }
                            _ => { Field::Boolean(false) }
                        }
                    }
                    _ => {
                        return Invalid(format!("Cannot apply {} to this values", "$id".to_string()));
                    }
                }

            }
        }
    }
}

define_cmp_oper!(And, |l,r| { l && r});
define_cmp_oper!(Or, |l,r| { l || r});



#[test]
fn test_bool_bool_and() {
    let l = Box::new(true);
    let r = Box::new(false);
    let op = And::new(l, r);
    assert!(matches!(op.get_result(), Field::Boolean(false)));
}

#[test]
fn test_bool_bool_or() {
    let l = Box::new(true);
    let r = Box::new(false);
    let op = Or::new(l, r);
    assert!(matches!(op.get_result(), Field::Boolean(true)));
}

#[test]
fn test_int_bool_and() {
    let l = Box::new(1);
    let r = Box::new(true);
    let op = And::new(l, r);
    assert!(matches!(op.get_result(), Invalid(_)));
}








