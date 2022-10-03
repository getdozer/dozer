use crate::common::error::{DozerSqlError, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunctionType {
    Avg,
    Count,
    Max,
    Median,
    Min,
    Sum,
    Stddev,
    Variance,
}

impl AggregateFunctionType {
    pub(crate) fn new(name: &str) -> Result<AggregateFunctionType> {
        Ok(match name {
            "avg" => AggregateFunctionType::Avg,
            "count" => AggregateFunctionType::Count,
            "max" => AggregateFunctionType::Max,
            "median" => AggregateFunctionType::Median,
            "min" => AggregateFunctionType::Min,
            "sum" => AggregateFunctionType::Sum,
            "stddev" => AggregateFunctionType::Stddev,
            "variance" => AggregateFunctionType::Variance,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Aggregate function: {}",
                    name
                )));
            }
        })
    }
}