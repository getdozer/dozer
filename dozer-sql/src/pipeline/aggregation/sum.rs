use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Sum;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::{argv, calculate_err_field};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{DozerDuration, Field, FieldType, Schema, SourceDefinition, TimeUnit};
use num_traits::FromPrimitive;

pub fn validate_sum(args: &[Expression], schema: &Schema) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Sum)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                Sum.to_string(),
                arg.return_type,
                FieldTypes::new(vec![
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Decimal,
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
pub struct SumAggregator {
    current_state: SumState,
    return_type: Option<FieldType>,
}

#[derive(Debug)]
pub struct SumState {
    pub(crate) int_state: i64,
    pub(crate) i128_state: i128,
    pub(crate) uint_state: u64,
    pub(crate) u128_state: u128,
    pub(crate) float_state: f64,
    pub(crate) decimal_state: Decimal,
    pub(crate) duration_state: std::time::Duration,
}

impl SumAggregator {
    pub fn new() -> Self {
        Self {
            current_state: SumState {
                int_state: 0_i64,
                i128_state: 0_i128,
                uint_state: 0_u64,
                u128_state: 0_u128,
                float_state: 0_f64,
                decimal_state: Decimal::from_f64(0_f64).unwrap(),
                duration_state: std::time::Duration::new(0, 0),
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
        Some(typ) => match typ {
            FieldType::UInt => {
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
            FieldType::U128 => {
                if decr {
                    for field in fields {
                        let val = calculate_err_field!(field.to_u128(), Sum, field);
                        current_state.u128_state -= val;
                    }
                } else {
                    for field in fields {
                        let val = calculate_err_field!(field.to_u128(), Sum, field);
                        current_state.u128_state += val;
                    }
                }
                Ok(Field::U128(current_state.u128_state))
            }
            FieldType::Int => {
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
            FieldType::I128 => {
                if decr {
                    for field in fields {
                        let val = calculate_err_field!(field.to_i128(), Sum, field);
                        current_state.i128_state -= val;
                    }
                } else {
                    for field in fields {
                        let val = calculate_err_field!(field.to_i128(), Sum, field);
                        current_state.i128_state += val;
                    }
                }
                Ok(Field::I128(current_state.i128_state))
            }
            FieldType::Float => {
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
            FieldType::Decimal => {
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
            FieldType::Duration => {
                if decr {
                    for field in fields {
                        let val = calculate_err_field!(field.to_duration()?, Sum, field);
                        current_state.duration_state -= val.0;
                    }
                } else {
                    for field in fields {
                        let val = calculate_err_field!(field.to_duration()?, Sum, field);
                        current_state.duration_state += val.0;
                    }
                }
                Ok(Field::Duration(DozerDuration(
                    current_state.duration_state,
                    TimeUnit::Nanoseconds,
                )))
            }
            FieldType::Boolean
            | FieldType::String
            | FieldType::Text
            | FieldType::Date
            | FieldType::Timestamp
            | FieldType::Binary
            | FieldType::Json
            | FieldType::Point => Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported return type {typ} for {Sum}"
            )))),
        },
        None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
            "Not supported None return type for {Sum}"
        )))),
    }
}
