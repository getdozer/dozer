use crate::execution::error::{DozerError, Result};
use crate::execution::expressions::comparators::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::execution::expressions::math_operators::{Diff, Div, Mod, Mult, Sum};
use crate::execution::expressions::values::{
    Field as SqlField, FieldValue, IntValue, NumericValue, Value, ValueTypes,
};
use crate::{Edge, EmptyProcessor, Field, Node};
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SelectItem, SetExpr, Statement, Value as SqlValue};

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
        let selection_node = PipelineBuilder::selection_to_node(select.selection)?;

        // Select clause
        let projection_node = PipelineBuilder::projection_to_node(select.projection)?;

        let nodes = vec![selection_node, projection_node];
        Ok((nodes, vec![]))
    }

    fn selection_to_node(selection: Option<Expr>) -> Result<Node> {
        match selection {
            Some(expression) => {
                let operator = PipelineBuilder::parse_sql_expression(expression)?;
                Ok(Node::new(100, Box::new(EmptyProcessor::new())))
            }
            _ => Err(DozerError::NotImplemented("Unsupported query.".to_string())),
        }
    }

    fn projection_to_node(projection: Vec<SelectItem>) -> Result<Node> {
        // todo: implement
        Ok(Node::new(200, Box::new(EmptyProcessor::new())))
    }

    fn parse_sql_expression(expression: Expr) -> Result<Box<dyn Value>> {
        match expression {
            Expr::Identifier(i) => Ok(Box::new(SqlField::new(i.to_string()))),
            Expr::Value(SqlValue::Number(n, _)) =>
                Ok(PipelineBuilder::parse_sql_number(&n)?),
            Expr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => Ok(Box::new(s)),
            Expr::BinaryOp { left, op, right } => Ok(PipelineBuilder::parse_sql_binary_op(*left, op, *right)?),

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
                    "Value is not numeric.".to_string(),
                )),
            },
        }
    }

    fn parse_sql_binary_op(left: Expr, op: BinaryOperator, right: Expr) -> Result<Box<dyn Value>> {
        let left_op = PipelineBuilder::parse_sql_expression(left)?;
        let right_op = PipelineBuilder::parse_sql_expression(right)?;
        match op {
            BinaryOperator::Gt => Ok(Box::new(Gt::new(left_op, right_op))),
            BinaryOperator::GtEq => Ok(Box::new(Gte::new(left_op, right_op))),
            BinaryOperator::Lt => Ok(Box::new(Lt::new(left_op, right_op))),
            BinaryOperator::LtEq => Ok(Box::new(Lte::new(left_op, right_op))),
            BinaryOperator::Eq => Ok(Box::new(Eq::new(left_op, right_op))),
            BinaryOperator::NotEq => Ok(Box::new(Ne::new(left_op, right_op))),
            BinaryOperator::Plus => Ok(Box::new(Sum::new(left_op, right_op))),
            BinaryOperator::Minus => Ok(Box::new(Diff::new(left_op, right_op))),
            BinaryOperator::Multiply => Ok(Box::new(Mult::new(left_op, right_op))),
            BinaryOperator::Divide => Ok(Box::new(Div::new(left_op, right_op))),
            BinaryOperator::Modulo => Ok(Box::new(Mod::new(left_op, right_op))),
            BinaryOperator::And => Err(DozerError::NotImplemented(
                "Unsupported operator AND".to_string(),
            )),
            BinaryOperator::Or => Err(DozerError::NotImplemented(
                "Unsupported operator OR".to_string(),
            )),
            // BinaryOperator::PGRegexMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexIMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotIMatch => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::BitwiseAnd => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::BitwiseOr => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftRight => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftLeft => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
            BinaryOperator::StringConcat => Err(DozerError::NotImplemented(
                "Unsupported operator CONCAT.".to_string(),
            )),
            _ => Err(DozerError::NotImplemented(
                "Unsupported operator.".to_string(),
            )),
        }
    }
}
