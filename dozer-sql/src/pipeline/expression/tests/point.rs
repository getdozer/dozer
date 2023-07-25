use crate::pipeline::expression::geo::point::{evaluate_point, validate_point};
use crate::pipeline::expression::tests::test_common::*;
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{DozerPoint, Field, FieldDefinition, FieldType, Schema, SourceDefinition};

use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidFunctionArgumentType, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::expression::execution::Expression;
use proptest::prelude::*;

#[test]
fn test_point() {
    proptest!(
        ProptestConfig::with_cases(1000), move |(x: i64, y: i64)| {
            test_validate_point(x, y);
            test_evaluate_point(x, y);
    });
}

fn test_validate_point(x: i64, y: i64) {
    let schema = Schema::default()
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
            Expression::Literal(Field::Int(y)),
        ],
        &schema,
    );

    let _expected_types = [FieldType::Float];
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
            Expression::Literal(Field::Int(x)),
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

fn test_evaluate_point(x: i64, y: i64) {
    let row = ProcessorRecord::new();

    let schema = Schema::default()
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
    let _fn_type = String::from("x");

    let result = evaluate_point(&schema, &[], &row);
    assert!(result.is_err());
    assert!(matches!(result, Err(InvalidArgument(_fn_type))));

    let _fn_type = String::from("y");

    let result = evaluate_point(&schema, &[Expression::Literal(Field::Int(x))], &row);
    assert!(result.is_err());
    assert!(matches!(result, Err(InvalidArgument(_fn_type))));

    let result = evaluate_point(
        &schema,
        &[
            Expression::Literal(Field::Int(x)),
            Expression::Literal(Field::Int(y)),
        ],
        &row,
    );

    assert!(result.is_ok());

    let result = evaluate_point(
        &schema,
        &[
            Expression::Literal(Field::Int(x)),
            Expression::Literal(Field::Null),
        ],
        &row,
    );

    assert!(result.is_ok());
    assert!(matches!(result, Ok(Field::Null)));

    let result = evaluate_point(
        &schema,
        &[
            Expression::Literal(Field::Null),
            Expression::Literal(Field::Int(y)),
        ],
        &row,
    );

    assert!(result.is_ok());
    assert!(matches!(result, Ok(Field::Null)));
}

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
