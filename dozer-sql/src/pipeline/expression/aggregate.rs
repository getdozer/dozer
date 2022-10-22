use dozer_types::errors::pipeline::PipelineError;
use dozer_types::errors::pipeline::PipelineError::InvalidFunction;

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
