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
use std::fmt::{Debug, Display, Formatter, Write};

pub trait Aggregator: Send + Sync {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError>;
    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError>;
    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError>;
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
    Min,
    Sum,
}

impl Display for AggregatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregatorType::Avg => f.write_str("avg"),
            AggregatorType::Count => f.write_str("count"),
            AggregatorType::Max => f.write_str("max"),
            AggregatorType::Min => f.write_str("min"),
            AggregatorType::Sum => f.write_str("sum"),
        }
    }
}

pub fn get_aggregator_from_aggregator_type(typ: AggregatorType) -> Box<dyn Aggregator> {
    match typ {
        AggregatorType::Avg => Box::new(AvgAggregator::new()),
        AggregatorType::Count => Box::new(CountAggregator::new()),
        AggregatorType::Max => Box::new(MaxAggregator::new()),
        AggregatorType::Min => Box::new(MinAggregator::new()),
        AggregatorType::Sum => Box::new(SumAggregator::new()),
    }
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
