use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunctionType {
    Avg,
    Count,
    Max,
    MaxAppendOnly,
    MaxValue,
    Min,
    MinAppendOnly,
    MinValue,
    Sum,
}

impl AggregateFunctionType {
    pub(crate) fn new(name: &str) -> Option<AggregateFunctionType> {
        match name {
            "avg" => Some(AggregateFunctionType::Avg),
            "count" => Some(AggregateFunctionType::Count),
            "max" => Some(AggregateFunctionType::Max),
            "max_append_only" => Some(AggregateFunctionType::MaxAppendOnly),
            "max_value" => Some(AggregateFunctionType::MaxValue),
            "min" => Some(AggregateFunctionType::Min),
            "min_append_only" => Some(AggregateFunctionType::MinAppendOnly),
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
            AggregateFunctionType::MaxAppendOnly => f.write_str("MAX_APPEND_ONLY"),
            AggregateFunctionType::MaxValue => f.write_str("MAX_VALUE"),
            AggregateFunctionType::Min => f.write_str("MIN"),
            AggregateFunctionType::MinAppendOnly => f.write_str("MIN_APPEND_ONLY"),
            AggregateFunctionType::MinValue => f.write_str("MIN_VALUE"),
            AggregateFunctionType::Sum => f.write_str("SUM"),
        }
    }
}
