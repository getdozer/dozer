use crate::execution::where_exp::{Operand, Operator};
use crate::{Edge, EmptyProcessor, Field, Node};
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SetExpr, Statement, Value};
use crate::execution::error::{Result, DozerError};

pub struct PipelineBuilder {
    ast: sqlparser::ast::Statement,
}

impl PipelineBuilder {
    pub fn statement_to_pipeline(statement: Statement) -> Result<(Vec<Node>, Vec<Edge>)> {
        match statement {
            Statement::Query(query) => PipelineBuilder::query_to_pipeline(*query),
            _ => Err(DozerError::NotImplemented("Unsupported type of Query.".to_string())),
        }
    }

    pub fn query_to_pipeline(query: Query) -> Result<(Vec<Node>, Vec<Edge>)> {
        PipelineBuilder::set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(set_expr: SetExpr) -> Result<(Vec<Node>, Vec<Edge>)> {
        match set_expr {
            SetExpr::Select(s) => PipelineBuilder::select_to_pipeline(*s),
            SetExpr::Query(q) => PipelineBuilder::query_to_pipeline(*q),
            _ => Err(DozerError::NotImplemented("Unsupported type of Query.".to_string())),
        }
    }

    fn select_to_pipeline(select: Select) -> Result<(Vec<Node>, Vec<Edge>)> {
        // Where clause
        let node = PipelineBuilder::selection_to_node(select.selection).unwrap();

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

    fn expression_to_operand(expression: Expr) -> Result<Operand> {
        match expression {
            Expr::Identifier(i) => Ok(Operand::field_value(1)),
            Expr::Value(Value::Number(n, _)) => Ok(Operand::const_value(PipelineBuilder::parse_sql_number(&n).unwrap())),
            Expr::Value(Value::Boolean(b)) => Ok(Operand::const_value(Field::bool_field(b))),
            _ => Err(DozerError::NotImplemented("Unsupported expression.".to_string())),
        }
    }

    fn parse_sql_number(n: &str) -> Result<Field> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Field::int_field(n)),
            Err(_) => Err(DozerError::NotImplemented("Unsupported value.".to_string())),
        }
    }

    fn parse_sql_binary_op(
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<Box<Operator>> {
        let operator = match op {
            BinaryOperator::Gt => Ok(Box::new(Operator::gt(
                PipelineBuilder::expression_to_operand(left).unwrap(),
                PipelineBuilder::expression_to_operand(right).unwrap(),
            ))),
            BinaryOperator::GtEq => Ok(Box::new(Operator::gte(
                PipelineBuilder::expression_to_operand(left).unwrap(),
                PipelineBuilder::expression_to_operand(right).unwrap(),
            ))),
            BinaryOperator::Lt => Ok(Box::new(Operator::lt(
                Operand::field_value(0),
                Operand::field_value(0),
            ))),
            BinaryOperator::LtEq => Ok(Box::new(Operator::lte(
                Operand::field_value(0),
                Operand::field_value(0),
            ))),
            BinaryOperator::Eq => Ok(Box::new(Operator::eq(
                Operand::field_value(0),
                Operand::field_value(0),
            ))),
            BinaryOperator::NotEq => Ok(Box::new(Operator::ne(
                Operand::field_value(0),
                Operand::field_value(0),
            ))),
            // BinaryOperator::Plus => Err("Unsupported query.".to_string())
            // BinaryOperator::Minus => Err("Unsupported query.".to_string())
            // BinaryOperator::Multiply => Err("Unsupported query.".to_string())
            // BinaryOperator::Divide => Err("Unsupported query.".to_string())
            // BinaryOperator::Modulo => Err("Unsupported query.".to_string())
            // BinaryOperator::And => PipelineBuilder::parse_and_binary_op(left, op, right),
            // BinaryOperator::Or => Operator::or(Operand::field_value(0), Operand::field_value(0)),
            // BinaryOperator::PGRegexMatch => Err("Unsupported query.".to_string())
            // BinaryOperator::PGRegexIMatch => Err("Unsupported query.".to_string())
            // BinaryOperator::PGRegexNotMatch => Err("Unsupported query.".to_string())
            // BinaryOperator::PGRegexNotIMatch => Err("Unsupported query.".to_string())
            // BinaryOperator::BitwiseAnd => Err("Unsupported query.".to_string())
            // BinaryOperator::BitwiseOr => Err("Unsupported query.".to_string())
            // BinaryOperator::PGBitwiseShiftRight => Err("Unsupported query.".to_string())
            // BinaryOperator::PGBitwiseShiftLeft => Err("Unsupported query.".to_string())
            // BinaryOperator::StringConcat => Err("Unsupported query.".to_string())
            _ => Err(DozerError::NotImplemented("Unsupported operator.".to_string())),
        };
        return operator;
    }

    fn parse_expression(expression: Expr) -> Result<Node> {

        Ok(Node::new(100, Box::new(EmptyProcessor::new())))
    }

    fn parse_and_binary_op(left: Expr, op: BinaryOperator, right: Expr) -> Node {
        //Operator::and(Operator::field_value(0),Operand::field_value(0))
        Node::new(100, Box::new(EmptyProcessor::new()))
    }
}
