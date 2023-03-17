use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Sum;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::{argv, calculate_err_field};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};
use num_traits::FromPrimitive;

pub fn validate_sum(args: &[Expression], schema: &Schema) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Sum)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Int,
        FieldType::UInt => FieldType::UInt,
        FieldType::Float => FieldType::Float,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                Sum.to_string(),
                r,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::Int,
                    FieldType::Float,
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
pub struct SumAggregator {
    current_state: SumState,
    return_type: Option<FieldType>,
}

#[derive(Debug)]
pub struct SumState {
    pub(crate) int_state: i64,
    pub(crate) uint_state: u64,
    pub(crate) float_state: f64,
    pub(crate) decimal_state: Decimal,
}

impl SumAggregator {
    pub fn new() -> Self {
        Self {
            current_state: SumState {
                int_state: 0_i64,
                uint_state: 0_u64,
                float_state: 0_f64,
                decimal_state: Decimal::from_f64(0_f64).unwrap(),
            },
            return_type: None,
        }
    }
}

impl Aggregator for SumAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        get_sum(old, &mut self.current_state, self.return_type, true)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        get_sum(new, &mut self.current_state, self.return_type, false)
    }
}

pub fn get_sum(
    fields: &[Field],
    current_state: &mut SumState,
    return_type: Option<FieldType>,
    decr: bool,
) -> Result<Field, PipelineError> {
    match return_type {
        Some(FieldType::UInt) => {
            if decr {
                for field in fields {
                    let val = calculate_err_field!(field.to_uint(), Sum, field);
                    current_state.uint_state -= val;
                }
            } else {
                for field in fields {
                    let val = calculate_err_field!(field.to_uint(), Sum, field);
                    current_state.uint_state += val;
                }
            }
            Ok(Field::UInt(current_state.uint_state))
        }
        Some(FieldType::Int) => {
            if decr {
                for field in fields {
                    let val = calculate_err_field!(field.to_int(), Sum, field);
                    current_state.int_state -= val;
                }
            } else {
                for field in fields {
                    let val = calculate_err_field!(field.to_int(), Sum, field);
                    current_state.int_state += val;
                }
            }
            Ok(Field::Int(current_state.int_state))
        }
        Some(FieldType::Float) => {
            if decr {
                for field in fields {
                    let val = calculate_err_field!(field.to_float(), Sum, field);
                    current_state.float_state -= val;
                }
            } else {
                for field in fields {
                    let val = calculate_err_field!(field.to_float(), Sum, field);
                    current_state.float_state += val;
                }
            }
            Ok(Field::Float(OrderedFloat::from(current_state.float_state)))
        }
        Some(FieldType::Decimal) => {
            if decr {
                for field in fields {
                    let val = calculate_err_field!(field.to_decimal(), Sum, field);
                    current_state.decimal_state -= val;
                }
            } else {
                for field in fields {
                    let val = calculate_err_field!(field.to_decimal(), Sum, field);
                    current_state.decimal_state += val;
                }
            }
            Ok(Field::Decimal(current_state.decimal_state))
        }
        Some(not_supported_return_type) => Err(PipelineError::InternalExecutionError(InvalidType(
            format!("Not supported return type {not_supported_return_type} for {Sum}"),
        ))),
        None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
            "Not supported None return type for {Sum}"
        )))),
    }
}
