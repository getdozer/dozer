use crate::common::error::{DozerSqlError, Result};


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunction {
    Avg,
    Count,
    Max,
    Median,
    Min,
    Sum,
    Stddev,
    Variance,
}

impl AggregateFunction {

    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match name {
            "avg" => AggregateFunction::Avg,
            "count" => AggregateFunction::Count,
            "max" => AggregateFunction::Max,
            "median" => AggregateFunction::Median,
            "min" => AggregateFunction::Min,
            "sum" => AggregateFunction::Sum,
            "stddev" => AggregateFunction::Stddev,
            "variance" => AggregateFunction::Variance,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported aggregate function: {}",
                    name
                )));
            }
        })
    }
}