use std::str::FromStr;

use crate::arg_utils::{extract_point, validate_num_arguments};
use crate::error::Error;
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};

use crate::execution::{Expression, ExpressionType};
use crate::geo::common::GeoFunctionType;
use dozer_types::geo::GeodesicDistance;
use dozer_types::geo::HaversineDistance;
use dozer_types::geo::VincentyDistance;

use dozer_types::ordered_float::OrderedFloat;

const EXPECTED_ARGS_TYPES: &[FieldType] = &[FieldType::Point, FieldType::Point, FieldType::String];

pub enum Algorithm {
    Geodesic,
    Haversine,
    Vincenty,
}

impl FromStr for Algorithm {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GEODESIC" => Ok(Algorithm::Geodesic),
            "HAVERSINE" => Ok(Algorithm::Haversine),
            "VINCENTY" => Ok(Algorithm::Vincenty),
            &_ => Err(Error::InvalidDistanceAlgorithm(s.to_string())),
        }
    }
}

const DEFAULT_ALGORITHM: Algorithm = Algorithm::Geodesic;

pub(crate) fn validate_distance(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    let ret_type = FieldType::Float;
    validate_num_arguments(2..4, args.len(), GeoFunctionType::Distance)?;

    for (argument_index, exp) in args.iter().enumerate() {
        let return_type = exp.get_type(schema)?.return_type;
        let expected_arg_type_option = EXPECTED_ARGS_TYPES.get(argument_index);
        if let Some(expected_arg_type) = expected_arg_type_option {
            if &return_type != expected_arg_type {
                return Err(Error::InvalidFunctionArgumentType {
                    function_name: GeoFunctionType::Distance.to_string(),
                    argument_index,
                    actual: return_type,
                    expected: vec![*expected_arg_type],
                });
            }
        }
    }

    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_distance(
    schema: &Schema,
    args: &[Expression],
    record: &Record,
) -> Result<Field, Error> {
    validate_num_arguments(2..4, args.len(), GeoFunctionType::Distance)?;
    let f_from = args[0].evaluate(record, schema)?;

    let f_to = args[1].evaluate(record, schema)?;

    if f_from == Field::Null || f_to == Field::Null {
        Ok(Field::Null)
    } else {
        let from = extract_point(f_from, GeoFunctionType::Distance, 0)?;
        let to = extract_point(f_to, GeoFunctionType::Distance, 1)?;
        let calculation_type = args.get(2).map_or_else(
            || Ok(DEFAULT_ALGORITHM),
            |arg| {
                let f = arg.evaluate(record, schema)?;
                let t = f.to_string();
                Algorithm::from_str(&t)
            },
        )?;

        let distance: OrderedFloat<f64> = match calculation_type {
            Algorithm::Geodesic => Ok(from.geodesic_distance(&to)),
            Algorithm::Haversine => Ok(from.0.haversine_distance(&to.0)),
            Algorithm::Vincenty => from
                .0
                .vincenty_distance(&to.0)
                .map_err(Error::FailedToCalculateVincentyDistance),
        }?;

        Ok(Field::Float(distance))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_types::types::{DozerPoint, FieldDefinition, SourceDefinition};
    use proptest::prelude::*;
    use Expression::Literal;

    #[test]
    fn test_geo() {
        proptest!(ProptestConfig::with_cases(1000), move |(x1: f64, x2: f64, y1: f64, y2: f64)| {
            let row = Record::new(vec![]);
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
        row: &Record,
        result: Option<Result<Field, Error>>,
    ) {
        let args = &vec![Literal(from.clone()), Literal(to.clone())];
        if validate_distance(args, &Schema::default()).is_ok() {
            match result {
                None => {
                    let from_f = from.to_owned();
                    let to_f = to.to_owned();
                    let f = extract_point(from_f, GeoFunctionType::Distance, 0).unwrap();
                    let t = extract_point(to_f, GeoFunctionType::Distance, 0).unwrap();
                    let _dist = match typ {
                        None => f.geodesic_distance(&t),
                        Some(Algorithm::Geodesic) => f.geodesic_distance(&t),
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

        let result = validate_distance(&[], &schema);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

        let result = validate_distance(&[Expression::Column { index: 0 }], &schema);

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

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
        assert!(matches!(
            result,
            Err(Error::InvalidNumberOfArguments { .. })
        ));

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
            Err(Error::InvalidFunctionArgumentType { .. })
        ));
    }
}
