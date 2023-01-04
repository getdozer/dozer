use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidFunction;
use std::fmt::{Display, Formatter};

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
    pub(crate) fn new(name: &str) -> Result<AggregateFunctionType, PipelineError> {
        match name {
            "avg" => Ok(AggregateFunctionType::Avg),
            "count" => Ok(AggregateFunctionType::Count),
            "max" => Ok(AggregateFunctionType::Max),
            "median" => Ok(AggregateFunctionType::Median),
            "min" => Ok(AggregateFunctionType::Min),
            "sum" => Ok(AggregateFunctionType::Sum),
            "stddev" => Ok(AggregateFunctionType::Stddev),
            "variance" => Ok(AggregateFunctionType::Variance),
            _ => Err(InvalidFunction(name.to_string())),
        }
    }
}

impl Display for AggregateFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunctionType::Avg => f.write_str("AVG"),
            AggregateFunctionType::Count => f.write_str("COUNT"),
            AggregateFunctionType::Max => f.write_str("MAX"),
            AggregateFunctionType::Median => f.write_str("MEDIAN"),
            AggregateFunctionType::Min => f.write_str("MIN"),
            AggregateFunctionType::Sum => f.write_str("SUM"),
            AggregateFunctionType::Stddev => f.write_str("STDDEV"),
            AggregateFunctionType::Variance => f.write_str("VARIANCE"),
        }
    }
}
