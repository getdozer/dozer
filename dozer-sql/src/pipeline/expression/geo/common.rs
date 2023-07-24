use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionType};

use crate::pipeline::expression::geo::distance::{evaluate_distance, validate_distance};
use crate::pipeline::expression::geo::point::{evaluate_point, validate_point};
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{Field, Schema};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum GeoFunctionType {
    Point,
    Distance,
}

impl Display for GeoFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GeoFunctionType::Point => f.write_str("POINT"),
            GeoFunctionType::Distance => f.write_str("DISTANCE"),
        }
    }
}

pub(crate) fn get_geo_function_type(
    function: &GeoFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    match function {
        GeoFunctionType::Point => validate_point(args, schema),
        GeoFunctionType::Distance => validate_distance(args, schema),
    }
}

impl GeoFunctionType {
    pub fn new(name: &str) -> Result<GeoFunctionType, PipelineError> {
        match name {
            "point" => Ok(GeoFunctionType::Point),
            "distance" => Ok(GeoFunctionType::Distance),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &[Expression],
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        match self {
            GeoFunctionType::Point => evaluate_point(schema, args, record),
            GeoFunctionType::Distance => evaluate_distance(schema, args, record),
        }
    }
}
