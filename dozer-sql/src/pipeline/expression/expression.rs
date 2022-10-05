use dozer_types::types::{Field, Record};

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::ScalarFunctionType;

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Column {
        index: usize
    },
    Literal(Field),
    UnaryOperator {
        operator: UnaryOperatorType,
        arg: Box<Expression>,
    },
    BinaryOperator {
        left: Box<Expression>,
        operator: BinaryOperatorType,
        right: Box<Expression>,
    },
    ScalarFunction {
        fun: ScalarFunctionType,
        args: Vec<Box<Expression>>,
    },
    AggregateFunction {
        fun: AggregateFunctionType,
        args: Vec<Box<Expression>>,
    },
}


pub trait PhysicalExpression: Send + Sync {
    fn evaluate(&self, record: &Record) -> Field;
}

impl PhysicalExpression for Expression {
    fn evaluate(&self, record: &Record) -> Field {
        match self {
            Expression::Literal(field) => field.clone(),
            Expression::Column { index } => record.values.get(*index).unwrap().clone(),
            Expression::BinaryOperator { left, operator, right } => operator.evaluate(left, right, record),
            Expression::ScalarFunction { fun, args } => fun.evaluate(args, record),
            _ => Field::Invalid(format!("Invalid Expression: {:?}", self))
        }
    }
}
