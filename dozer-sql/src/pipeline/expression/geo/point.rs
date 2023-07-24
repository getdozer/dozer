use crate::arg_float;
use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidFunctionArgumentType, NotEnoughArguments, TooManyArguments,
};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{DozerPoint, Field, FieldType, Schema};

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::pipeline::expression::geo::common::GeoFunctionType;

pub(crate) fn validate_point(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let ret_type = FieldType::Point;
    let expected_arg_type = FieldType::Float;

    if args.len() < 2 {
        return Err(NotEnoughArguments(GeoFunctionType::Point.to_string()));
    }

    if args.len() > 2 {
        return Err(TooManyArguments(GeoFunctionType::Point.to_string()));
    }

    for (idx, exp) in args.iter().enumerate() {
        let return_type = exp.get_type(schema)?.return_type;
        if return_type != expected_arg_type {
            return Err(InvalidFunctionArgumentType(
                GeoFunctionType::Point.to_string(),
                return_type,
                FieldTypes::new(vec![expected_arg_type]),
                idx,
            ));
        }
    }

    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_point(
    schema: &Schema,
    args: &[Expression],
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let _res_type = FieldType::Point;
    let f_x = args
        .get(0)
        .ok_or(InvalidArgument("x".to_string()))?
        .evaluate(record, schema)?;
    let f_y = args
        .get(1)
        .ok_or(InvalidArgument("y".to_string()))?
        .evaluate(record, schema)?;

    if f_x == Field::Null || f_y == Field::Null {
        Ok(Field::Null)
    } else {
        let x = arg_float!(f_x, GeoFunctionType::Point, 0)?;
        let y = arg_float!(f_y, GeoFunctionType::Point, 0)?;

        Ok(Field::Point(DozerPoint::from((x, y))))
    }
}
