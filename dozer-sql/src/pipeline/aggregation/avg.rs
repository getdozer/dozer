use crate::argv;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::aggregation::sum::{get_sum, SumState};
use crate::pipeline::errors::PipelineError::InvalidValue;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Avg;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use dozer_types::arrow::datatypes::ArrowNativeTypeOp;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{DozerDuration, Field, FieldType, Schema, SourceDefinition, TimeUnit};
use num_traits::FromPrimitive;

use std::ops::Div;

pub fn validate_avg(args: &[Expression], schema: &Schema) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Avg)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::Decimal,
        FieldType::U128 => FieldType::Decimal,
        FieldType::Int => FieldType::Decimal,
        FieldType::I128 => FieldType::Decimal,
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
                Avg.to_string(),
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
pub struct AvgAggregator {
    current_state: SumState,
    current_count: u64,
    return_type: Option<FieldType>,
}

impl AvgAggregator {
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
            current_count: 0_u64,
            return_type: None,
        }
    }
}

impl Aggregator for AvgAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        self.current_count -= old.len() as u64;
        get_average(
            old,
            &mut self.current_state,
            &mut self.current_count,
            self.return_type,
            true,
        )
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        self.current_count += new.len() as u64;
        get_average(
            new,
            &mut self.current_state,
            &mut self.current_count,
            self.return_type,
            false,
        )
    }
}

fn get_average(
    field: &[Field],
    current_sum: &mut SumState,
    current_count: &mut u64,
    return_type: Option<FieldType>,
    decr: bool,
) -> Result<Field, PipelineError> {
    let sum = get_sum(field, current_sum, return_type, decr)?;

    match return_type {
        Some(typ) => match typ {
            FieldType::UInt => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let u_sum = sum
                    .to_uint()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::UInt(u_sum.div_wrapping(*current_count)))
            }
            FieldType::U128 => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let u_sum = sum
                    .to_u128()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::U128(u_sum.wrapping_div(*current_count as u128)))
            }
            FieldType::Int => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let i_sum = sum
                    .to_int()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::Int(i_sum.div_wrapping(*current_count as i64)))
            }
            FieldType::I128 => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let i_sum = sum
                    .to_i128()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::I128(i_sum.div_wrapping(*current_count as i128)))
            }
            FieldType::Float => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let f_sum = sum
                    .to_float()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::Float(OrderedFloat(
                    f_sum.div_wrapping(*current_count as f64),
                )))
            }
            FieldType::Decimal => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let d_sum = sum
                    .to_decimal()
                    .ok_or(InvalidValue(sum.to_string().unwrap()))
                    .unwrap();
                Ok(Field::Decimal(d_sum.div(Decimal::from(*current_count))))
            }
            FieldType::Duration => {
                if *current_count == 0 {
                    return Ok(Field::Null);
                }
                let str_dur = sum.to_duration()?.unwrap().to_string();
                let d_sum = sum
                    .to_duration()?
                    .ok_or(InvalidValue(str_dur.clone()))
                    .unwrap();

                Ok(Field::Duration(DozerDuration(
                    d_sum
                        .0
                        .checked_div((*current_count) as u32)
                        .ok_or(InvalidValue(str_dur))
                        .unwrap(),
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
            | FieldType::Point => Err(PipelineError::InvalidReturnType(format!(
                "Not supported return type {typ} for {Avg}"
            ))),
        },
        None => Err(PipelineError::InvalidReturnType(format!(
            "Not supported None return type for {Avg}"
        ))),
    }
}
