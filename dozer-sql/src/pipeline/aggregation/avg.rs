use crate::pipeline::aggregation::aggregator::{Aggregator};
use crate::pipeline::aggregation::sum::{get_sum, SumState};
use crate::pipeline::errors::PipelineError::{InvalidValue};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Avg;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::{argv};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::arrow::datatypes::ArrowNativeTypeOp;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};
use num_traits::FromPrimitive;

use std::ops::Div;

pub fn validate_avg(args: &[Expression], schema: &Schema) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Avg)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Decimal,
        FieldType::UInt => FieldType::Decimal,
        FieldType::Float => FieldType::Float,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                Avg.to_string(),
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
                uint_state: 0_u64,
                float_state: 0_f64,
                decimal_state: Decimal::from_f64(0_f64).unwrap(),
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
        self.current_count -= 1;
        get_average(
            old,
            &mut self.current_state,
            &mut self.current_count,
            self.return_type,
            true,
        )
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        self.current_count += 1;
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
        Some(FieldType::UInt) => {
            if *current_count == 0 {
                return Ok(Field::UInt(0));
            }
            let u_sum = sum
                .to_uint()
                .ok_or(InvalidValue(sum.to_string().unwrap()))
                .unwrap();
            Ok(Field::UInt(u_sum.div_wrapping(*current_count)))
        }
        Some(FieldType::Int) => {
            if *current_count == 0 {
                return Ok(Field::Int(0));
            }
            let i_sum = sum
                .to_int()
                .ok_or(InvalidValue(sum.to_string().unwrap()))
                .unwrap();
            Ok(Field::Int(i_sum.div_wrapping(*current_count as i64)))
        }
        Some(FieldType::Float) => {
            if *current_count == 0 {
                return Ok(Field::Float(OrderedFloat(0.0)));
            }
            let f_sum = sum
                .to_float()
                .ok_or(InvalidValue(sum.to_string().unwrap()))
                .unwrap();
            Ok(Field::Float(OrderedFloat(
                f_sum.div_wrapping(*current_count as f64),
            )))
        }
        Some(FieldType::Decimal) => {
            if *current_count == 0 {
                return Ok(Field::Decimal(Decimal::new(0, 0)));
            }
            let d_sum = sum
                .to_decimal()
                .ok_or(InvalidValue(sum.to_string().unwrap()))
                .unwrap();
            Ok(Field::Decimal(d_sum.div(Decimal::from(*current_count))))
        }
        Some(not_supported_return_type) => Err(PipelineError::InternalExecutionError(InvalidType(
            format!("Not supported return type {not_supported_return_type} for {Avg}"),
        ))),
        None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
            "Not supported None return type for {Avg}"
        )))),
    }
}
