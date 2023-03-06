use crate::pipeline::aggregation::avg::AvgAggregator;
use crate::pipeline::aggregation::count::CountAggregator;
use crate::pipeline::aggregation::max::MaxAggregator;
use crate::pipeline::aggregation::min::MinAggregator;
use crate::pipeline::aggregation::sum::SumAggregator;
use crate::pipeline::errors::PipelineError;

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::execution::Expression;
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::types::{Field, FieldType, Schema};
use std::fmt::{Display, Formatter};
use hashbrown::HashMap;

pub trait Aggregator {
    fn update(
        &self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError>;
    fn delete(&self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError>;
    fn insert(&self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregatorType {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl Display for AggregatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Aggregator::Avg => f.write_str("avg"),
            Aggregator::Count => f.write_str("count"),
            Aggregator::Max => f.write_str("max"),
            Aggregator::Min => f.write_str("min"),
            Aggregator::Sum => f.write_str("sum"),
        }
    }
}

pub fn get_aggregator_from_aggregator_type(typ: AggregatorType) -> Box<dyn Aggregator> {
    Box::new(match typ {
        AggregatorType::Avg => AvgAggregator::new(),
        AggregatorType::Count => CountAggregator::new(),
        AggregatorType::Max => MaxAggregator::new(),
        AggregatorType::Min => MinAggregator::new(),
        AggregatorType::Sum => SumAggregator::new(),
    })
}

pub fn get_aggregator_type_from_aggregation_expression(
    e: &Expression,
    schema: &Schema,
) -> Result<(Expression, AggregatorType), PipelineError> {
    match e {
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Sum,
            args,
        } => Ok((
            args.get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Sum.to_string())
                })?
                .clone(),
            AggregatorType::Sum,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Min,
            args,
        } => Ok((
            args.get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Min.to_string())
                })?
                .clone(),
            AggregatorType::Min,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Max,
            args,
        } => Ok((
            args.get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Max.to_string())
                })?
                .clone(),
            AggregatorType::Max,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Avg,
            args,
        } => Ok((
            args.get(0)
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Avg.to_string())
                })?
                .clone(),
            AggregatorType::Avg,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Count,
            args: _,
        } => Ok((Expression::Literal(Field::Int(0)), AggregatorType::Count)),
        _ => Err(PipelineError::InvalidFunction(e.to_string(schema))),
    }
}

pub fn update_map(
    field: &Field,
    val_delta: u64,
    decr: bool,
    field_hash: &mut HashMap<Field, u64>,
) -> u64 {
    let get_prev_count = field_hash.get(field);
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
        field_hash.remove(field);
    } else {
        field_hash.insert(field.clone(), new_count);
    }
    new_count
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
