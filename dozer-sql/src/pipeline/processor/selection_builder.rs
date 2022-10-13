use anyhow::{bail, Result};
use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, UnaryOperator as SqlUnaryOperator,
    Value as SqlValue,
};

use dozer_types::types::{Field, Schema};

use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};

pub struct SelectionBuilder {}

impl SelectionBuilder {
    pub fn build_expression(
        &self,
        selection: &Option<SqlExpr>,
        schema: &Schema,
    ) -> Result<Box<Expression>> {
        match selection {
            Some(expression) => Ok(self.parse_sql_expression(expression, schema)?),
            None => Ok(Box::new(Expression::Literal(Field::Boolean(true)))),
        }
    }

    pub fn column_index(&self, name: &String, schema: &Schema) -> Result<usize> {
        let schema_idx: HashMap<String, usize> = schema
            .fields
            .iter()
            .enumerate()
            .map(|e| (e.1.name.clone(), e.0))
            .collect();

        if let Some(index) = schema_idx.get(name).cloned() {
            Ok(index)
        } else {
            bail!("The Field {} does not exists", &name)
        }
    }

    pub fn parse_sql_expression(
        &self,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>> {
        match expression {
            SqlExpr::Identifier(ident) => Ok(Box::new(Expression::Column {
                index: self.column_index(&ident.value, schema)?,
            })),
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok(Box::new(Expression::Literal(Field::String(s.to_string()))))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(left, op, right, schema)?)
            }
            SqlExpr::UnaryOp { op, expr } => Ok(self.parse_sql_unary_op(op, expr, schema)?),
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr, schema)?),
            _ => bail!("Unsupported Expression.".to_string(),),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<Box<Expression>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(Expression::Literal(Field::Int(n)))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(Expression::Literal(Field::Float(f)))),
                Err(_) => bail!("Value is not Numeric.".to_string(),),
            },
        }
    }

    fn parse_sql_unary_op(
        &self,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>> {
        let arg = self.parse_sql_expression(expr, schema)?;

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => bail!("Unsupported SQL unary operator {:?}", op),
        };

        Ok(Box::new(Expression::UnaryOperator { operator, arg }))
    }

    fn parse_sql_binary_op(
        &self,
        left_expr: &SqlExpr,
        op: &SqlBinaryOperator,
        right_expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>> {
        let left = self.parse_sql_expression(left_expr, schema)?;
        let right = self.parse_sql_expression(right_expr, schema)?;

        let operator = match op {
            SqlBinaryOperator::Gt => BinaryOperatorType::Gt,
            SqlBinaryOperator::GtEq => BinaryOperatorType::Gte,
            SqlBinaryOperator::Lt => BinaryOperatorType::Lt,
            SqlBinaryOperator::LtEq => BinaryOperatorType::Lte,
            SqlBinaryOperator::Eq => BinaryOperatorType::Eq,
            SqlBinaryOperator::NotEq => BinaryOperatorType::Ne,

            SqlBinaryOperator::Plus => BinaryOperatorType::Add,
            SqlBinaryOperator::Minus => BinaryOperatorType::Sub,
            SqlBinaryOperator::Multiply => BinaryOperatorType::Mul,
            SqlBinaryOperator::Divide => BinaryOperatorType::Div,
            SqlBinaryOperator::Modulo => BinaryOperatorType::Mod,

            SqlBinaryOperator::And => BinaryOperatorType::And,
            SqlBinaryOperator::Or => BinaryOperatorType::Or,

            // BinaryOperator::BitwiseAnd => ...
            // BinaryOperator::BitwiseOr => ...
            // BinaryOperator::StringConcat => ...
            _ => bail!("Unsupported SQL Binary Operator {:?}", op),
        };

        Ok(Box::new(Expression::BinaryOperator {
            left,
            operator,
            right,
        }))
    }
}
