use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::number::evaluate_round;
use crate::pipeline::expression::scalar::tests::scalar_common::run_scalar_fct;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema};

#[test]
fn test_abs() {
    let f = run_scalar_fct(
        "SELECT ABS(c) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("c"), FieldType::Int, false),
                false,
            )
            .clone(),
        vec![Field::Int(-1)],
    );
    assert_eq!(f, Field::Int(1));
}

#[test]
fn test_round() {
    let row = Record::new(None, vec![]);

    let v = Box::new(Literal(Field::Int(1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&Schema::empty(), &v, Some(d), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1)
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.1))));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&Schema::empty(), &v, Some(d), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(2.0))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.6))));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&Schema::empty(), &v, Some(d), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(3.0))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
    let d = &Box::new(Literal(Field::Int(2)));
    assert_eq!(
        evaluate_round(&Schema::empty(), &v, Some(d), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(2.63))
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
}
