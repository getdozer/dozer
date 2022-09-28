use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::PhysicalExpression;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum OperatorType {
    Sum,
}

impl OperatorType {

}