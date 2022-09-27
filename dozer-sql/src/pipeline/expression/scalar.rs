use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::operator::Expression;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunction {
    Abs,
    Power,
    Round,
}

impl ScalarFunction {

    pub fn new(name: &str, args: Vec<Box<dyn Expression>>) -> Result<ScalarFunction> {
        Ok(match name {
            "abs" => ScalarFunction::Abs,
            "power" => ScalarFunction::Power,
            "round" => ScalarFunction::Round,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported scalar function: {}",
                    name
                )));
            }
        })
    }
}