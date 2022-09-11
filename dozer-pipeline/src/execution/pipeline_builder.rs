use crate::execution::error::{DozerError, Result};
use crate::execution::expressions::comparators::Eq as EqOperator;
use crate::execution::expressions::values::{
    Field as SqlField, FieldValue, IntValue, Value, ValueTypes,
};
use crate::execution::where_exp::{Operand, Operator};
use crate::{Edge, EmptyProcessor, Field, Node};
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SetExpr, Statement, Value as SqlValue};

pub struct PipelineBuilder {
    ast: sqlparser::ast::Statement,
}

impl PipelineBuilder {
    pub fn statement_to_pipeline(statement: Statement) -> Result<(Vec<Node>, Vec<Edge>)> {
        match statement {
            Statement::Query(query) => PipelineBuilder::query_to_pipeline(*query),
            _ => Err(DozerError::NotImplemented(
                "Unsupported type of Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(query: Query) -> Result<(Vec<Node>, Vec<Edge>)> {
        PipelineBuilder::set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(set_expr: SetExpr) -> Result<(Vec<Node>, Vec<Edge>)> {
        match set_expr {
            SetExpr::Select(s) => PipelineBuilder::select_to_pipeline(*s),
            SetExpr::Query(q) => PipelineBuilder::query_to_pipeline(*q),
            _ => Err(DozerError::NotImplemented(
                "Unsupported type of Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(select: Select) -> Result<(Vec<Node>, Vec<Edge>)> {
        // Where clause
        let node = PipelineBuilder::selection_to_node(select.selection)?;

        let nodes = vec![node];
        Ok((nodes, vec![]))
    }

    fn selection_to_node(selection: Option<Expr>) -> Result<Node> {
        match selection.unwrap() {
            Expr::BinaryOp { left, op, right } => {
                let operator = PipelineBuilder::parse_sql_binary_op(*left, op, *right);
                Ok(Node::new(100, Box::new(EmptyProcessor::new())))
            }
            _ => Err(DozerError::NotImplemented("Unsupported query.".to_string())),
        }
    }

    fn expression_to_operand(expression: Expr) -> Result<Box<dyn Value>> {
        match expression {
            Expr::Identifier(i) => Ok(Box::new(SqlField::new(i.to_string()))),
            Expr::Value(SqlValue::Number(n, _)) => {
                Ok(PipelineBuilder::parse_sql_number(&n).unwrap())
            }
            Expr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok(Box::new(s))
            }
            _ => Err(DozerError::NotImplemented(
                "Unsupported expression.".to_string(),
            )),
        }
    }

    fn parse_sql_number(n: &str) -> Result<Box<dyn Value>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(n)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(f)),
                Err(_) => Err(DozerError::NotImplemented(
                    "Unsupported expression.".to_string(),
                )),
            },
        }
    }

    fn parse_sql_binary_op(left: Expr, op: BinaryOperator, right: Expr) -> Result<Box<dyn Value>> {
        match op {
            BinaryOperator::Gt => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
            BinaryOperator::GtEq => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
            BinaryOperator::Lt => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
            BinaryOperator::LtEq => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
            BinaryOperator::Eq => Ok(Box::new(EqOperator::new(
                PipelineBuilder::expression_to_operand(left).unwrap(),
                PipelineBuilder::expression_to_operand(right).unwrap(),
            ))),
            // BinaryOperator::NotEq => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Plus => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Minus => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Multiply => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Divide => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Modulo => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::And => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::Or => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexIMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotIMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::BitwiseAnd => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::BitwiseOr => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftRight => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftLeft => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            BinaryOperator::StringConcat => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
            _ => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
        }
    }
}
