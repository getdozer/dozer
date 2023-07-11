use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidFunction;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunctionType {
    Avg,
    Count,
    Max,
    MaxValue,
    Min,
    Sum,
}

impl AggregateFunctionType {
    pub(crate) fn new(name: &str) -> Result<AggregateFunctionType, PipelineError> {
        match name {
            "avg" => Ok(AggregateFunctionType::Avg),
            "count" => Ok(AggregateFunctionType::Count),
            "max" => Ok(AggregateFunctionType::Max),
            "max_value" => Ok(AggregateFunctionType::MaxValue),
            "min" => Ok(AggregateFunctionType::Min),
            "sum" => Ok(AggregateFunctionType::Sum),
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
            AggregateFunctionType::MaxValue => f.write_str("MAX_VALUE"),
            AggregateFunctionType::Min => f.write_str("MIN"),
            AggregateFunctionType::Sum => f.write_str("SUM"),
        }
    }
}
