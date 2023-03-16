use proptest::prelude::*;
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::number::evaluate_round;
use crate::pipeline::expression::scalar::tests::scalar_common::run_scalar_fct;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};

#[test]
fn test_abs() {
    proptest!(ProptestConfig::with_cases(100), |(x in 0i64..100000000i64)| {
        let f = run_scalar_fct(
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
            vec![Field::Int(-x)],
        );
        assert_eq!(f, Field::Int(x));
    });
}

#[test]
fn test_round() {
    proptest!(ProptestConfig::with_cases(100), |(i_num in 0i64..100000000i64, f_num in 0f64..100000000f64)| {
        let row = Record::new(None, vec![], None);

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
        let d = &Box::new(Literal(Field::Int(2)));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(format!("{:.2}", f_num).parse::<f64>().unwrap()))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(212.633))));
        let d = &Box::new(Literal(Field::Int(-2)));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(200.0))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
        let d = &Box::new(Literal(Field::Float(OrderedFloat(2.1))));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(2.63))
        );

        let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
        let d = &Box::new(Literal(Field::String("2.3".to_string())));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(3.0))
        );

        let v = Box::new(Literal(Field::Null));
        let d = &Box::new(Literal(Field::String("2".to_string())));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        let v = Box::new(Literal(Field::Null));
        let d = &Box::new(Literal(Field::String("2".to_string())));
        assert_eq!(
            evaluate_round(&Schema::empty(), &v, Some(d), &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}
