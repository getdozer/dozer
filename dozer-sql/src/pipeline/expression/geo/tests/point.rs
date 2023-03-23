use crate::pipeline::expression::geo::point::validate_point;
use crate::pipeline::expression::geo::tests::geo_common::run_geo_fct;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{DozerPoint, Field, FieldDefinition, FieldType, Schema, SourceDefinition};

use crate::pipeline::errors::PipelineError::{
    InvalidFunctionArgumentType, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::expression::execution::Expression;

// use proptest::prelude::*;
//
// fn point_strat() -> impl Strategy<Value = DozerPoint> {
//     (any<f64>::(), any<f64>::()).prop_map(|(x, y)| {
//         DozerPoint::from((x, y))
//     })
// }

#[test]
fn test_validate_point() {
    let schema = Schema::empty()
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
        .clone();
    let _fn_type = String::from("POINT");

    let result = validate_point(&[], &schema);
    assert!(result.is_err());
    assert!(matches!(result, Err(NotEnoughArguments(_fn_type))));

    let result = validate_point(&[Expression::Column { index: 0 }], &schema);

    assert!(result.is_err());
    assert!(matches!(result, Err(NotEnoughArguments(_fn_type))));

    let result = validate_point(
        &[
            Expression::Column { index: 0 },
            Expression::Column { index: 1 },
        ],
        &schema,
    );

    assert!(result.is_ok());

    let result = validate_point(
        &[
            Expression::Column { index: 0 },
            Expression::Column { index: 1 },
            Expression::Column { index: 2 },
        ],
        &schema,
    );

    assert!(result.is_err());
    assert!(matches!(result, Err(TooManyArguments(_fn_type))));

    let result = validate_point(
        &[
            Expression::Column { index: 0 },
            Expression::Literal(Field::Int(1)),
        ],
        &schema,
    );

    let _expected_types = vec![FieldType::Float];
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(InvalidFunctionArgumentType(
            _fn_type,
            FieldType::Int,
            _expected_types,
            1
        ))
    ));

    let result = validate_point(
        &[
            Expression::Literal(Field::Int(1)),
            Expression::Column { index: 0 },
        ],
        &schema,
    );

    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(InvalidFunctionArgumentType(
            _fn_type,
            FieldType::Int,
            _expected_types,
            0
        ))
    ));
}

#[test]
fn test_point() {
    let f = run_geo_fct(
        "SELECT POINT(x, y) FROM LOCATION",
        Schema::empty()
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
    let f = run_geo_fct(
        "SELECT POINT(x, y) FROM LOCATION",
        Schema::empty()
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
