use crate::pipeline::errors::PipelineError::{
    InvalidFunctionArgumentType, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::geo::distance::validate_distance;
use crate::pipeline::expression::geo::tests::geo_common::run_geo_fct;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{DozerPoint, Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_validate_distance() {
    let schema = Schema::empty()
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
    let _fn_type = String::from("DISTANCE");

    let result = validate_distance(&[], &schema);
    assert!(result.is_err());
    assert!(matches!(result, Err(NotEnoughArguments(_fn_type))));

    let result = validate_distance(&[Expression::Column { index: 0 }], &schema);

    assert!(result.is_err());
    assert!(matches!(result, Err(NotEnoughArguments(_fn_type))));

    let result = validate_distance(
        &[
            Expression::Column { index: 0 },
            Expression::Column { index: 1 },
        ],
        &schema,
    );

    assert!(result.is_ok());

    let result = validate_distance(
        &[
            Expression::Column { index: 0 },
            Expression::Column { index: 1 },
            Expression::Literal(Field::String("GEODESIC".to_string())),
        ],
        &schema,
    );

    assert!(result.is_ok());

    let result = validate_distance(
        &[
            Expression::Column { index: 0 },
            Expression::Column { index: 1 },
            Expression::Literal(Field::String("GEODESIC".to_string())),
            Expression::Column { index: 2 },
        ],
        &schema,
    );

    assert!(result.is_err());
    assert!(matches!(result, Err(TooManyArguments(_fn_type))));

    let result = validate_distance(
        &[
            Expression::Column { index: 0 },
            Expression::Literal(Field::String("GEODESIC".to_string())),
            Expression::Column { index: 2 },
        ],
        &schema,
    );

    let _expected_types = vec![FieldType::Point];
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(InvalidFunctionArgumentType(
            _fn_type,
            FieldType::String,
            _expected_types,
            1
        ))
    ));
}

#[test]
fn test_distance() {
    let tests = vec![
        ("", 1113.0264976969002),
        ("GEODESIC", 1113.0264976969002),
        ("HAVERSINE", 1111.7814468418496),
        ("VINCENTY", 1113.0264975564357),
    ];

    let schema = Schema::empty()
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
        if let Field::Float(OrderedFloat(result)) = run_geo_fct(&sql, schema.clone(), input.clone())
        {
            assert!((result - expected_result) < 0.000000001);
        } else {
            panic!("Expected float");
        }
    }
}

#[test]
fn test_distance_with_nullable_parameter() {
    let f = run_geo_fct(
        "SELECT DISTANCE(from, to) FROM LOCATION",
        Schema::empty()
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
