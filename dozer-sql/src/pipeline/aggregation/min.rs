use crate::pipeline::aggregation::aggregator::{update_map, Aggregator};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Min;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::{argv, calculate_err, calculate_err_field};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};
use std::collections::BTreeMap;

pub fn validate_min(args: &[Expression], schema: &Schema) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Min)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Bson
        | FieldType::Point => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                Min.to_string(),
                arg.return_type,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ]),
                0,
            ));
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

#[derive(Debug)]
pub struct MinAggregator {
    current_state: BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
}

impl MinAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MinAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_map(old, 1_u64, true, &mut self.current_state);
        get_min(&self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_map(new, 1_u64, false, &mut self.current_state);
        get_min(&self.current_state, self.return_type)
    }
}

fn get_min(
    field_map: &BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().min(), Min).clone();
        match return_type {
            Some(typ) => match typ {
                FieldType::UInt => Ok(Field::UInt(calculate_err_field!(val.to_uint(), Min, val))),
                FieldType::U128 => Ok(Field::U128(calculate_err_field!(val.to_u128(), Min, val))),
                FieldType::Int => Ok(Field::Int(calculate_err_field!(val.to_int(), Min, val))),
                FieldType::I128 => Ok(Field::I128(calculate_err_field!(val.to_i128(), Min, val))),
                FieldType::Float => Ok(Field::Float(OrderedFloat::from(calculate_err_field!(
                    val.to_float(),
                    Min,
                    val
                )))),
                FieldType::Decimal => Ok(Field::Decimal(calculate_err_field!(
                    val.to_decimal(),
                    Min,
                    val
                ))),
                FieldType::Timestamp => Ok(Field::Timestamp(calculate_err_field!(
                    val.to_timestamp()?,
                    Min,
                    val
                ))),
                FieldType::Date => Ok(Field::Date(calculate_err_field!(val.to_date()?, Min, val))),
                FieldType::Duration => Ok(Field::Duration(calculate_err_field!(
                    val.to_duration()?,
                    Min,
                    val
                ))),
                FieldType::Boolean
                | FieldType::String
                | FieldType::Text
                | FieldType::Binary
                | FieldType::Bson
                | FieldType::Point => Err(PipelineError::InternalExecutionError(InvalidType(
                    format!("Not supported return type {typ} for {Min}"),
                ))),
            },
            None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported None return type for {Min}"
            )))),
        }
    }
}
