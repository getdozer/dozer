use crate::arg_utils::{extract_float, validate_num_arguments};
use crate::error::Error;
use dozer_types::types::Record;
use dozer_types::types::{DozerPoint, Field, FieldType, Schema};

use crate::execution::{Expression, ExpressionType};
use crate::geo::common::GeoFunctionType;

pub fn validate_point(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let ret_type = FieldType::Point;
    let expected_arg_type = FieldType::Float;

    validate_num_arguments(2..3, args.len(), GeoFunctionType::Point)?;

    for (argument_index, exp) in args.iter().enumerate() {
        let return_type = exp.get_type(schema)?.return_type;
        if return_type != expected_arg_type {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: GeoFunctionType::Point.to_string(),
                argument_index,
                actual: return_type,
                expected: vec![expected_arg_type],
            });
        }
    }

    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub fn evaluate_point(
    schema: &Schema,
    args: &mut [Expression],
    record: &Record,
) -> Result<Field, Error> {
    validate_num_arguments(2..3, args.len(), GeoFunctionType::Point)?;
    let f_x = args[0].evaluate(record, schema)?;
    let f_y = args[1].evaluate(record, schema)?;

    if f_x == Field::Null || f_y == Field::Null {
        Ok(Field::Null)
    } else {
        let x = extract_float(f_x, GeoFunctionType::Point, 0)?;
        let y = extract_float(f_y, GeoFunctionType::Point, 1)?;

        Ok(Field::Point(DozerPoint::from((x, y))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_types::types::{FieldDefinition, SourceDefinition};
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

        let result = validate_point(&[], &schema);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

        let result = validate_point(&[Expression::Column { index: 0 }], &schema);

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

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
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

        let result = validate_point(
            &[
                Expression::Column { index: 0 },
                Expression::Literal(Field::Int(y)),
            ],
            &schema,
        );

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidFunctionArgumentType { .. })
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
            Err(Error::InvalidFunctionArgumentType { .. })
        ));
    }

    fn test_evaluate_point(x: i64, y: i64) {
        let row = Record::new(vec![]);

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

        let result = evaluate_point(&schema, &mut [], &row);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

        let result = evaluate_point(&schema, &mut [Expression::Literal(Field::Int(x))], &row);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

        let result = evaluate_point(
            &schema,
            &mut [
                Expression::Literal(Field::Int(x)),
                Expression::Literal(Field::Int(y)),
            ],
            &row,
        );

        assert!(result.is_ok());

        let result = evaluate_point(
            &schema,
            &mut [
                Expression::Literal(Field::Int(x)),
                Expression::Literal(Field::Null),
            ],
            &row,
        );

        assert!(result.is_ok());
        assert!(matches!(result, Ok(Field::Null)));

        let result = evaluate_point(
            &schema,
            &mut [
                Expression::Literal(Field::Null),
                Expression::Literal(Field::Int(y)),
            ],
            &row,
        );

        assert!(result.is_ok());
        assert!(matches!(result, Ok(Field::Null)));
    }
}
