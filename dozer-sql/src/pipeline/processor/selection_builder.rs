use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, UnaryOperator as SqlUnaryOperator,
    Value as SqlValue,
};

use dozer_types::errors::pipeline::PipelineError;
use dozer_types::errors::pipeline::PipelineError::{
    InvalidExpression, InvalidOperator, InvalidValue,
};

use dozer_types::types::{Field, Schema};

use crate::pipeline::expression::builder::{parse_sql_number, Bypass};
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};

use super::common::column_index;

pub struct SelectionBuilder {}

impl SelectionBuilder {
    pub fn build_expression(
        &self,
        selection: &Option<SqlExpr>,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        match selection {
            Some(expression) => {
                let (expression, bypass) = self.parse_sql_expression(expression, schema)?;
                Ok(expression)
            }
            None => Ok(Box::new(Expression::Literal(Field::Boolean(true)))),
        }
    }

    pub fn parse_sql_expression(
        &self,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, Bypass), PipelineError> {
        match expression {
            SqlExpr::Identifier(ident) => {
                let idx = column_index(&ident.value, schema)?;
                Ok((Box::new(Expression::Column { index: idx }), false))
            }
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(parse_sql_number(n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok((
                    Box::new(Expression::Literal(Field::String(s.to_string()))),
                    false,
                ))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(left, op, right, schema)?)
            }
            SqlExpr::UnaryOp { op, expr } => Ok(self.parse_sql_unary_op(op, expr, schema)?),
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr, schema)?),
            _ => Err(InvalidExpression(
                "Expression {:?} is not valid!".to_string(),
            )),
        }
    }

    fn parse_sql_unary_op(
        &self,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, Bypass), PipelineError> {
        let arg = self.parse_sql_expression(expr, schema)?;

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => return Err(InvalidOperator(format!("{:?}", op))),
        };

        Ok((
            Box::new(Expression::UnaryOperator {
                operator,
                arg: arg.0,
            }),
            false,
        ))
    }

    fn parse_sql_binary_op(
        &self,
        left_expr: &SqlExpr,
        op: &SqlBinaryOperator,
        right_expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, Bypass), PipelineError> {
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
            _ => return Err(InvalidOperator(format!("{:?}", op))),
        };

        Ok((
            Box::new(Expression::BinaryOperator {
                left: left.0,
                operator,
                right: right.0,
            }),
            false,
        ))
    }
}
