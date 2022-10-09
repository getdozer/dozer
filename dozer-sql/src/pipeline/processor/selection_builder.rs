use std::collections::HashMap;
use anyhow::bail;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, TableFactor, TableWithJoins, UnaryOperator as SqlUnaryOperator, Value as SqlValue};

use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_types::types::{Field, Schema};

use crate::common::error::{DozerSqlError, Result};
use crate::common::utils::normalize_ident;
use crate::pipeline::expression::expression::Expression;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::processor::selection::SelectionProcessorFactory;

pub struct SelectionBuilder {
    schema_idx: HashMap<String, usize>,
}

impl SelectionBuilder {
    pub fn new(schema: &Schema) -> SelectionBuilder {
        Self {
            schema_idx: schema.fields.iter().enumerate().map(|e| (e.1.name.clone(), e.0)).collect(),
        }
    }

    pub fn get_processor(&self, selection: &Option<SqlExpr>, from: &Vec<TableWithJoins>) -> Result<SelectionProcessorFactory> {
        match selection {
            Some(expression) => {
                let expression = self.parse_sql_expression(&expression)?;
                let input_ports = self.get_input_ports(from)?;

                Ok(SelectionProcessorFactory::new(0, input_ports, vec![DefaultPortHandle], expression))
            }
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported WHERE clause.".to_string(),
            )),
        }
    }

    pub fn parse_sql_expression(&self, expression: &SqlExpr) -> Result<Box<Expression>> {
        match expression {
            SqlExpr::Identifier(ident) => {
                Ok(Box::new(
                    Expression::Column { index: *self.schema_idx.get(&ident.value).unwrap() }))
            }
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok(Box::new(Expression::Literal(Field::String(s.to_string()))))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(left, op, right)?)
            }
            SqlExpr::UnaryOp { op, expr } => {
                Ok(self.parse_sql_unary_op(op, expr)?)
            }
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr)?),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Expression.".to_string(),
            )),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<Box<Expression>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(Expression::Literal(Field::Int(n)))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(Expression::Literal(Field::Float(f)))),
                Err(_) => Err(DozerSqlError::NotImplemented(format!(
                    "Value is not Numeric.",
                ))),
            },
        }
    }

    fn parse_sql_unary_op(&self, op: &SqlUnaryOperator, expr: &SqlExpr) -> Result<Box<Expression>> {
        let arg = self.parse_sql_expression(expr)?;

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => return Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL unary operator {:?}", op
            )))
        };

        Ok(Box::new(Expression::UnaryOperator { operator, arg }))

    }

    fn parse_sql_binary_op(&self,
                           left_expr: &SqlExpr,
                           op: &SqlBinaryOperator,
                           right_expr: &SqlExpr,
    ) -> Result<Box<Expression>> {
        let left = self.parse_sql_expression(left_expr)?;
        let right = self.parse_sql_expression(right_expr)?;


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

            _ => return Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL Binary Operator {:?}", op
            ))),
        };

        Ok(Box::new(Expression::BinaryOperator { left, operator, right, }))

    }

    fn get_input_ports(&self, from: &Vec<TableWithJoins>) -> Result<Vec<u16>> {
        let mut input_ports = vec![];
        let counter:u16 = 0;
        for table in from.into_iter() {
            if let Ok(_) = self.get_input_name(table) {
                input_ports.push(counter);
            }
        }
        Ok(input_ports)
    }

    fn get_input_name(&self, table: &TableWithJoins) -> anyhow::Result<String> {
        match &table.relation {
            TableFactor::Table { name, alias: _, .. } => {
                let input_name = name.0.iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");

                Ok(input_name)

            }
            _ => bail!("Unsupported Table Name.")
        }
    }
}
