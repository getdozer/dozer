use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::number::{evaluate_abs, evaluate_round};
use crate::pipeline::expression::tests::test_common::*;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};
use proptest::prelude::*;
use std::ops::Neg;

#[test]
fn test_abs() {
    proptest!(ProptestConfig::with_cases(1000), |(i_num in 0i64..100000000i64, f_num in 0f64..100000000f64)| {
        let row = Record::new(None, vec![]);

        let v = Box::new(Literal(Field::Int(i_num.neg())));
        assert_eq!(
            evaluate_abs(&Schema::empty(), &v, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int(i_num)
        );

        let row = Record::new(None, vec![]);

        let v = Box::new(Literal(Field::Float(OrderedFloat(f_num.neg()))));
        assert_eq!(
            evaluate_abs(&Schema::empty(), &v, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num))
        );
    });
}

#[test]
fn test_round() {
    proptest!(ProptestConfig::with_cases(1000), |(i_num: i64, f_num: f64, i_pow: i32, f_pow: f32)| {
        let row = Record::new(None, vec![]);

        let v = Box::new(Literal(Field::Int(i_num)));
        let d = &Box::new(Literal(Field::Int(0)));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int(i_num)
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
        let d = &Box::new(Literal(Field::Int(0)));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num.round()))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
        let d = &Box::new(Literal(Field::Int(i_pow as i64)));
        let order = 10.0_f64.powi(i_pow);
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat((f_num * order).round() / order))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
        let d = &Box::new(Literal(Field::Float(OrderedFloat(f_pow as f64))));
        let order = 10.0_f64.powi(f_pow.round() as i32);
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat((f_num * order).round() / order))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
        let d = &Box::new(Literal(Field::String(f_pow.to_string())));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num.round()))
        );

        let v = Box::new(Literal(Field::Null));
        let d = &Box::new(Literal(Field::String(i_pow.to_string())));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_abs_logic() {
    proptest!(ProptestConfig::with_cases(1000), |(i_num in 0i64..100000000i64)| {
        let f = run_fct(
            "SELECT ABS(c) FROM USERS",
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        String::from("c"),
                        FieldType::Int,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    false,
                )
                .clone(),
            vec![Field::Int(i_num.neg())],
        );
        assert_eq!(f, Field::Int(i_num));
    });
}
