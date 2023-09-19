use crate::expression::tests::test_common::*;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{DozerPoint, Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_point_logical() {
    let f = run_fct(
        "SELECT POINT(x, y) FROM LOCATION",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("x"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("y"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![
            Field::Float(OrderedFloat(1.0)),
            Field::Float(OrderedFloat(2.0)),
        ],
    );
    assert_eq!(f, Field::Point(DozerPoint::from((1.0, 2.0))));
}

#[test]
fn test_point_with_nullable_parameter() {
    let f = run_fct(
        "SELECT POINT(x, y) FROM LOCATION",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("x"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("y"),
                    FieldType::Float,
                    true,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Float(OrderedFloat(1.0)), Field::Null],
    );
    assert_eq!(f, Field::Null);
}
