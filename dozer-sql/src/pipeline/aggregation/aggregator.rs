#![allow(clippy::enum_variant_names)]

use crate::pipeline::aggregation::avg::AvgAggregator;
use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::max::MaxAggregator;
use crate::pipeline::aggregation::min::MinAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;
use enum_dispatch::enum_dispatch;
use std::collections::BTreeMap;

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::execution::Expression;

use crate::pipeline::aggregation::max_value::MaxValueAggregator;
use crate::pipeline::aggregation::min_value::MinValueAggregator;
use crate::pipeline::errors::PipelineError::{InvalidFunctionArgument, InvalidValue};
use crate::pipeline::expression::aggregate::AggregateFunctionType::MaxValue;
use dozer_types::types::{Field, FieldType, Schema};
use std::fmt::{Debug, Display, Formatter};

#[enum_dispatch]
pub trait Aggregator: Send + Sync {
    fn init(&mut self, return_type: FieldType);
    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError>;
    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError>;
    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError>;
}

#[enum_dispatch(Aggregator)]
#[derive(Debug)]
pub enum AggregatorEnum {
    AvgAggregator,
    MinAggregator,
    MinValueAggregator,
    MaxAggregator,
    MaxValueAggregator,
    SumAggregator,
    CountAggregator,
}

impl Debug for dyn Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Aggregator")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregatorType {
    Avg,
    Count,
    Max,
    MaxValue,
    Min,
    MinValue,
    Sum,
}

impl Display for AggregatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregatorType::Avg => f.write_str("avg"),
            AggregatorType::Count => f.write_str("count"),
            AggregatorType::Max => f.write_str("max"),
            AggregatorType::MaxValue => f.write_str("max_value"),
            AggregatorType::Min => f.write_str("min"),
            AggregatorType::MinValue => f.write_str("min_value"),
            AggregatorType::Sum => f.write_str("sum"),
        }
    }
}

pub fn get_aggregator_from_aggregator_type(typ: AggregatorType) -> AggregatorEnum {
    match typ {
        AggregatorType::Avg => AvgAggregator::new().into(),
        AggregatorType::Count => CountAggregator::new().into(),
        AggregatorType::Max => MaxAggregator::new().into(),
        AggregatorType::MaxValue => MaxValueAggregator::new().into(),
        AggregatorType::Min => MinAggregator::new().into(),
        AggregatorType::MinValue => MinValueAggregator::new().into(),
        AggregatorType::Sum => SumAggregator::new().into(),
    }
}

pub fn get_aggregator_type_from_aggregation_expression(
    e: &Expression,
    schema: &Schema,
) -> Result<(Vec<Expression>, AggregatorType), PipelineError> {
    match e {
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Sum,
            args,
        } => Ok((
            vec![args
                .get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Sum.to_string())
                })?
                .clone()],
            AggregatorType::Sum,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Min,
            args,
        } => Ok((
            vec![args
                .get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Min.to_string())
                })?
                .clone()],
            AggregatorType::Min,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Max,
            args,
        } => Ok((
            vec![args
                .get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Max.to_string())
                })?
                .clone()],
            AggregatorType::Max,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MaxValue,
            args,
        } => Ok((
            vec![
                args.get(0)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MaxValue.to_string(),
                        )
                    })?
                    .clone(),
                args.get(1)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MaxValue.to_string(),
                        )
                    })?
                    .clone(),
            ],
            AggregatorType::MaxValue,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MinValue,
            args,
        } => Ok((
            vec![
                args.get(0)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MinValue.to_string(),
                        )
                    })?
                    .clone(),
                args.get(1)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MinValue.to_string(),
                        )
                    })?
                    .clone(),
            ],
            AggregatorType::MinValue,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Avg,
            args,
        } => Ok((
            vec![args
                .get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Avg.to_string())
                })?
                .clone()],
            AggregatorType::Avg,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Count,
            args,
        } => Ok((
            vec![args
                .get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Count.to_string())
                })?
                .clone()],
            AggregatorType::Count,
        )),
        _ => Err(PipelineError::InvalidFunction(e.to_string(schema))),
    }
}

pub fn update_map(
    fields: &[Field],
    val_delta: u64,
    decr: bool,
    field_map: &mut BTreeMap<Field, u64>,
) {
    for field in fields {
        if field == &Field::Null {
            continue;
        }

        let get_prev_count = field_map.get(field);
        let prev_count = match get_prev_count {
            Some(v) => *v,
            None => 0_u64,
        };
        let mut new_count = prev_count;
        if decr {
            new_count = new_count.wrapping_sub(val_delta);
        } else {
            new_count = new_count.wrapping_add(val_delta);
        }
        if new_count < 1 {
            field_map.remove(field);
        } else if field_map.contains_key(field) {
            if let Some(val) = field_map.get_mut(field) {
                *val = new_count;
            }
        } else {
            field_map.insert(field.clone(), new_count);
        }
    }
}

pub fn update_val_map(
    fields: &[Field],
    val_delta: u64,
    decr: bool,
    field_map: &mut BTreeMap<Field, u64>,
    return_map: &mut BTreeMap<Field, Vec<Field>>,
) -> Result<(), PipelineError> {
    let field = match fields.get(0) {
        Some(v) => v,
        None => {
            return Err(InvalidFunctionArgument(
                MaxValue.to_string(),
                Field::Null,
                0,
            ))
        }
    };
    let return_field = match fields.get(1) {
        Some(v) => v.clone(),
        None => {
            return Err(InvalidFunctionArgument(
                MaxValue.to_string(),
                Field::Null,
                0,
            ))
        }
    };

    if field == &Field::Null {
        return Ok(());
    }

    let get_prev_count = field_map.get(field);
    let prev_count = match get_prev_count {
        Some(v) => *v,
        None => 0_u64,
    };
    let mut new_count = prev_count;
    if decr {
        new_count = new_count.wrapping_sub(val_delta);
    } else {
        new_count = new_count.wrapping_add(val_delta);
    }
    if new_count < 1 {
        field_map.remove(field);
    } else if field_map.contains_key(field) {
        if let Some(val) = field_map.get_mut(field) {
            *val = new_count;
        }
    } else {
        field_map.insert(field.clone(), new_count);
    }

    if !decr {
        if return_map.contains_key(field) {
            if let Some(val) = return_map.get_mut(field) {
                val.insert(0, return_field);
            }
        } else {
            return_map.insert(field.clone(), vec![return_field]);
        }
    } else if return_map.contains_key(field) {
        if let Some(val) = return_map.get_mut(field) {
            let idx = match val.iter().position(|r| r.clone() == return_field) {
                Some(id) => id,
                None => return Err(InvalidValue(format!("{:?}", val))),
            };
            val.remove(idx);
            if val.is_empty() {
                return_map.remove(field);
            }
        }
    }

    let _res = format!("{:?}", field_map);
    let _res_return = format!("{:?}", return_map);

    Ok(())
}

#[macro_export]
macro_rules! deserialize_u8 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => u8::from_be_bytes(deserialize!(v)),
            None => 0_u8,
        }
    };
}

#[macro_export]
macro_rules! check_nan_f64 {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            0_f64
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! check_nan_decimal {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            dozer_types::rust_decimal::Decimal::zero()
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! try_unwrap {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[macro_export]
macro_rules! calculate_err {
    ($stmt:expr, $aggr:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {}",
            $aggr
        )))?
    };
}

#[macro_export]
macro_rules! calculate_err_field {
    ($stmt:expr, $aggr:expr, $field:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {} while parsing {}",
            $aggr, $field
        )))?
    };
}

#[macro_export]
macro_rules! calculate_err_type {
    ($stmt:expr, $aggr:expr, $return_type:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {} while casting {}",
            $aggr, $return_type
        )))?
    };
}
