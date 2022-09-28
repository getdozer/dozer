use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::PhysicalExpression;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Power,
    Round,
}

impl ScalarFunctionType {

    pub fn new(name: &str) -> Result<ScalarFunctionType> {
        Ok(match name {
            "abs" => ScalarFunctionType::Abs,
            "power" => ScalarFunctionType::Power,
            "round" => ScalarFunctionType::Round,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Scalar function: {}",
                    name
                )));
            }
        })
    }
}