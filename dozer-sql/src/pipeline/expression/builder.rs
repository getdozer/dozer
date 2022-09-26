use crate::common::error::{DozerSqlError, Result};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement, UnaryOperator, Value as SqlValue};
use dozer_types::types::{Field, Operation, OperationEvent, Record, Schema};
use crate::pipeline::expression::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::pipeline::expression::logical::{And, Not, Or};
use crate::pipeline::expression::mathematical::{Add, Div, Mod, Mul, Sub};
use crate::pipeline::expression::operator::{Column, Expression};


pub struct ExpressionBuilder {
    schema: Schema,
}

impl ExpressionBuilder {

    pub fn new(schema: Schema) -> ExpressionBuilder {
        Self {
            schema
        }
    }

    pub fn parse_sql_expression(&self, expression: &SqlExpr) -> Result<Box<dyn Expression>> {
        match expression {
            SqlExpr::Identifier(ident) => {
                Ok(Box::new(Column::new(*self.schema.get_column_index(ident.clone().value).unwrap())))
            },
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok(Box::new(s.clone()))
            },
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(left, op, right)?)
            },
            SqlExpr::UnaryOp { op, expr } => {
                Ok(self.parse_sql_unary_op(op, expr)?)
            },
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr)?),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Expression.".to_string(),
            )),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<Box<dyn Expression>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(n)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(f)),
                Err(_) => Err(DozerSqlError::NotImplemented(
                    "Value is not numeric.".to_string(),
                )),
            },
        }
    }

    fn parse_sql_unary_op(&self, op: &UnaryOperator, expr: &SqlExpr) -> Result<Box<dyn Expression>> {

        let expr_op = self.parse_sql_expression(expr)?;

        match op {
            UnaryOperator::Not => Ok(Box::new(Not::new(expr_op))),
            UnaryOperator::Plus => Err(DozerSqlError::NotImplemented(
                "Unsupported operator PLUS.".to_string(),
            )),
            UnaryOperator::Minus => Err(DozerSqlError::NotImplemented(
                "Unsupported operator MINUS.".to_string(),
            )),
            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL unary operator {:?}", op
            ))),
        }
    }

    fn parse_sql_binary_op(&self,
                           left: &SqlExpr,
                           op: &BinaryOperator,
                           right: &SqlExpr,
    ) -> Result<Box<dyn Expression>> {
        let left_op = self.parse_sql_expression(left)?;
        let right_op = self.parse_sql_expression(right)?;
        match op {
            BinaryOperator::Gt => Ok(Box::new(Gt::new(left_op, right_op))),
            BinaryOperator::GtEq => Ok(Box::new(Gte::new(left_op, right_op))),
            BinaryOperator::Lt => Ok(Box::new(Lt::new(left_op, right_op))),
            BinaryOperator::LtEq => Ok(Box::new(Lte::new(left_op, right_op))),
            BinaryOperator::Eq => Ok(Box::new(Eq::new(left_op, right_op))),
            BinaryOperator::NotEq => Ok(Box::new(Ne::new(left_op, right_op))),
            BinaryOperator::Plus => Ok(Box::new(Add::new(left_op, right_op))),
            BinaryOperator::Minus => Ok(Box::new(Sub::new(left_op, right_op))),
            BinaryOperator::Multiply => Ok(Box::new(Mul::new(left_op, right_op))),
            BinaryOperator::Divide => Ok(Box::new(Div::new(left_op, right_op))),
            BinaryOperator::Modulo => Ok(Box::new(Mod::new(left_op, right_op))),
            BinaryOperator::And => Ok(Box::new(And::new(left_op, right_op))),
            BinaryOperator::Or => Ok(Box::new(Or::new(left_op, right_op))),
            BinaryOperator::BitwiseAnd => Err(DozerSqlError::NotImplemented(
                "Unsupported operator BITWISE AND.".to_string(),
            )),
            BinaryOperator::BitwiseOr => Err(DozerSqlError::NotImplemented(
                "Unsupported operator BITWISE OR.".to_string(),
            )),
            BinaryOperator::StringConcat => Err(DozerSqlError::NotImplemented(
                "Unsupported operator CONCAT.".to_string(),
            )),
            // BinaryOperator::PGRegexMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexIMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotIMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftRight => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftLeft => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}", op
            ))),
        }
    }
}