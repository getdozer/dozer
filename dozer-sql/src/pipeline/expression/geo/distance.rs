use dozer_types::errors::types::TypeError::DistanceCalculationError;
use std::str::FromStr;

use crate::pipeline::errors::PipelineError::{
    InvalidFunctionArgumentType, InvalidValue, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::{arg_point, arg_str};
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{Field, FieldType, Schema};

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::pipeline::expression::geo::common::GeoFunctionType;
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
    type Err = PipelineError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GEODESIC" => Ok(Algorithm::Geodesic),
            "HAVERSINE" => Ok(Algorithm::Haversine),
            "VINCENTY" => Ok(Algorithm::Vincenty),
            &_ => Err(InvalidValue(s.to_string())),
        }
    }
}

const DEFAULT_ALGORITHM: Algorithm = Algorithm::Geodesic;

pub(crate) fn validate_distance(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let ret_type = FieldType::Float;
    if args.len() < 2 {
        return Err(NotEnoughArguments(GeoFunctionType::Distance.to_string()));
    }

    if args.len() > 3 {
        return Err(TooManyArguments(GeoFunctionType::Distance.to_string()));
    }

    for (idx, exp) in args.iter().enumerate() {
        let return_type = exp.get_type(schema)?.return_type;
        let expected_arg_type_option = EXPECTED_ARGS_TYPES.get(idx);
        if let Some(expected_arg_type) = expected_arg_type_option {
            if &return_type != expected_arg_type {
                return Err(InvalidFunctionArgumentType(
                    GeoFunctionType::Distance.to_string(),
                    return_type,
                    FieldTypes::new(vec![*expected_arg_type]),
                    idx,
                ));
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
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let f_from = args
        .get(0)
        .ok_or(InvalidValue(String::from("from")))?
        .evaluate(record, schema)?;

    let f_to = args
        .get(1)
        .ok_or(InvalidValue(String::from("to")))?
        .evaluate(record, schema)?;

    if f_from == Field::Null || f_to == Field::Null {
        Ok(Field::Null)
    } else {
        let from = arg_point!(f_from, GeoFunctionType::Distance, 0)?;
        let to = arg_point!(f_to, GeoFunctionType::Distance, 0)?;
        let calculation_type = args.get(2).map_or_else(
            || Ok(DEFAULT_ALGORITHM),
            |arg| {
                let f = arg.evaluate(record, schema)?;
                let t = arg_str!(f, GeoFunctionType::Distance, 0)?;
                Algorithm::from_str(&t)
            },
        )?;

        let distance: OrderedFloat<f64> = match calculation_type {
            Algorithm::Geodesic => Ok(from.geodesic_distance(to)),
            Algorithm::Haversine => Ok(from.0.haversine_distance(&to.0)),
            Algorithm::Vincenty => from
                .0
                .vincenty_distance(&to.0)
                .map_err(DistanceCalculationError),
        }?;

        Ok(Field::Float(distance))
    }
}
