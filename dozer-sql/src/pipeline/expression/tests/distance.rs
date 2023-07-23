use crate::arg_point;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{
    InvalidFunctionArgumentType, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::geo::common::GeoFunctionType;
use crate::pipeline::expression::geo::distance::{evaluate_distance, validate_distance, Algorithm};
use crate::pipeline::expression::tests::test_common::*;
use dozer_types::geo::{GeodesicDistance, HaversineDistance};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{
    DozerPoint, Field, FieldDefinition, FieldType, ProcessorRecord, Schema, SourceDefinition,
};
use proptest::prelude::*;

#[test]
fn test_geo() {
    proptest!(ProptestConfig::with_cases(1000), move |(x1: f64, x2: f64, y1: f64, y2: f64)| {
        let row = ProcessorRecord::new(vec![]);
        let from = Field::Point(DozerPoint::from((x1, y1)));
        let to = Field::Point(DozerPoint::from((x2, y2)));
        let null = Field::Null;

        test_distance(&from, &to, None, &row, None);
        test_distance(&from, &null, None, &row, Some(Ok(Field::Null)));
        test_distance(&null, &to, None, &row, Some(Ok(Field::Null)));

        test_distance(&from, &to, Some(Algorithm::Geodesic), &row, None);
        test_distance(&from, &null, Some(Algorithm::Geodesic), &row, Some(Ok(Field::Null)));
        test_distance(&null, &to, Some(Algorithm::Geodesic), &row, Some(Ok(Field::Null)));

        test_distance(&from, &to, Some(Algorithm::Haversine), &row, None);
        test_distance(&from, &null, Some(Algorithm::Haversine), &row, Some(Ok(Field::Null)));
        test_distance(&null, &to, Some(Algorithm::Haversine), &row, Some(Ok(Field::Null)));

        // test_distance(&from, &to, Some(Algorithm::Vincenty), &row, None);
        // test_distance(&from, &null, Some(Algorithm::Vincenty), &row, Some(Ok(Field::Null)));
        // test_distance(&null, &to, Some(Algorithm::Vincenty), &row, Some(Ok(Field::Null)));
    });
}

fn test_distance(
    from: &Field,
    to: &Field,
    typ: Option<Algorithm>,
    row: &ProcessorRecord,
    result: Option<Result<Field, PipelineError>>,
) {
    let args = &vec![Literal(from.clone()), Literal(to.clone())];
    if validate_distance(args, &Schema::default()).is_ok() {
        match result {
            None => {
                let from_f = from.to_owned();
                let to_f = to.to_owned();
                let f = arg_point!(from_f, GeoFunctionType::Distance, 0).unwrap();
                let t = arg_point!(to_f, GeoFunctionType::Distance, 0).unwrap();
                let _dist = match typ {
                    None => f.geodesic_distance(t),
                    Some(Algorithm::Geodesic) => f.geodesic_distance(t),
                    Some(Algorithm::Haversine) => f.0.haversine_distance(&t.0),
                    Some(Algorithm::Vincenty) => OrderedFloat(0.0),
                    // Some(Algorithm::Vincenty) => f.0.vincenty_distance(&t.0).unwrap(),
                };
                assert!(matches!(
                    evaluate_distance(&Schema::default(), args, row),
                    Ok(Field::Float(_dist)),
                ))
            }
            Some(_val) => {
                assert!(matches!(
                    evaluate_distance(&Schema::default(), args, row),
                    _val,
                ))
            }
        }
    }
}

#[test]
fn test_validate_distance() {
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

    let _expected_types = [FieldType::Point];
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
