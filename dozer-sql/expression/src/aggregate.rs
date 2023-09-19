use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunctionType {
    Avg,
    Count,
    Max,
    MaxValue,
    Min,
    MinValue,
    Sum,
}

impl AggregateFunctionType {
    pub(crate) fn new(name: &str) -> Option<AggregateFunctionType> {
        match name {
            "avg" => Some(AggregateFunctionType::Avg),
            "count" => Some(AggregateFunctionType::Count),
            "max" => Some(AggregateFunctionType::Max),
            "max_value" => Some(AggregateFunctionType::MaxValue),
            "min" => Some(AggregateFunctionType::Min),
            "min_value" => Some(AggregateFunctionType::MinValue),
            "sum" => Some(AggregateFunctionType::Sum),
            _ => None,
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
            AggregateFunctionType::MinValue => f.write_str("MIN_VALUE"),
            AggregateFunctionType::Sum => f.write_str("SUM"),
        }
    }
}
