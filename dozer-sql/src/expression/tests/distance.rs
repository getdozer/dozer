use crate::expression::tests::test_common::*;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{DozerPoint, Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_distance_logical() {
    let tests = vec![
        ("", 1113.0264976969),
        ("GEODESIC", 1113.0264976969),
        ("HAVERSINE", 1111.7814468418496),
        ("VINCENTY", 1113.0264975564357),
    ];

    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                String::from("from"),
                FieldType::Point,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("to"),
                FieldType::Point,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let input = vec![
        Field::Point(DozerPoint::from((1.0, 1.0))),
        Field::Point(DozerPoint::from((1.01, 1.0))),
    ];

    for (calculation_type, expected_result) in tests {
        let sql = if calculation_type.is_empty() {
            "SELECT DISTANCE(from, to) FROM LOCATIONS".to_string()
        } else {
            format!("SELECT DISTANCE(from, to, '{calculation_type}') FROM LOCATIONS")
        };
        if let Field::Float(OrderedFloat(result)) = run_fct(&sql, schema.clone(), input.clone()) {
            assert!((result - expected_result) < 0.000000001);
        } else {
            panic!("Expected float");
        }
    }
}

#[test]
fn test_distance_with_nullable_parameter() {
    let f = run_fct(
        "SELECT DISTANCE(from, to) FROM LOCATION",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("from"),
                    FieldType::Point,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("to"),
                    FieldType::Point,
                    true,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Point(DozerPoint::from((0.0, 1.0))), Field::Null],
    );

    assert_eq!(f, Field::Null);
}
