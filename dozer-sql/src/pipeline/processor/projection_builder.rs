use anyhow::{bail, Result};
use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_types::types::Field;
use dozer_types::types::Schema;

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::ScalarFunction;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use crate::pipeline::processor::projection::ProjectionProcessorFactory;

pub struct ProjectionBuilder {
    schema_idx: HashMap<String, usize>,
}

impl ProjectionBuilder {
    pub fn new(schema: &Schema) -> ProjectionBuilder {
        Self {
            schema_idx: schema
                .fields
                .iter()
                .enumerate()
                .map(|e| (e.1.name.clone(), e.0))
                .collect(),
        }
    }

    pub fn get_processor(&self, projection: &[SelectItem]) -> Result<ProjectionProcessorFactory> {
        let expressions = projection
            .iter()
            .map(|expr| self.parse_sql_select_item(expr))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expression>>>()?;

        let names = projection
            .iter()
            .map(|item| self.get_select_item_name(item))
            .collect::<Result<Vec<String>>>();

        Ok(ProjectionProcessorFactory::new(
            vec![DefaultPortHandle],
            vec![DefaultPortHandle],
            expressions,
            names.unwrap(),
        ))
    }

    fn get_select_item_name(&self, item: &SelectItem) -> Result<String> {
        match item {
            SelectItem::UnnamedExpr(expr) => Ok(expr.to_string()),
            SelectItem::ExprWithAlias { expr: _, alias } => Ok(alias.to_string()),
            SelectItem::Wildcard => bail!("Unsupported Wildcard Operator"),
            SelectItem::QualifiedWildcard(ref object_name) => {
                bail!("Unsupported Qualified Wildcard Operator {}", object_name)
            }
        }
    }

    fn parse_sql_select_item(&self, sql: &SelectItem) -> Result<Vec<Expression>> {
        match sql {
            SelectItem::UnnamedExpr(sql_expr) => match self.parse_sql_expression(sql_expr) {
                Ok(expr) => Ok(vec![*expr.0]),
                Err(error) => Err(error),
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                bail!("Unsupported Expression {}:{}", expr, alias)
            }
            SelectItem::Wildcard => bail!("Unsupported Wildcard"),
            SelectItem::QualifiedWildcard(ref object_name) => {
                bail!("Unsupported Qualified Wildcard {}", object_name)
            }
        }
    }

    fn parse_sql_expression(&self, expression: &SqlExpr) -> Result<(Box<Expression>, bool)> {
        match expression {
            SqlExpr::Identifier(ident) => Ok((
                Box::new(Expression::Column {
                    index: *self.schema_idx.get(&ident.value).unwrap(),
                }),
                false,
            )),
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok((
                    Box::new(Expression::Literal(Field::String(s.clone()))),
                    false,
                ))
            }
            SqlExpr::UnaryOp { expr, op } => Ok(self.parse_sql_unary_op(op, expr)?),
            SqlExpr::BinaryOp { left, op, right } => Ok(self.parse_sql_binary_op(left, op, right)?),
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr)?),
            SqlExpr::Function(sql_function) => {
                let name = sql_function.name.to_string().to_lowercase();

                if let Ok(function) = ScalarFunctionType::new(&name) {
                    let mut arg_exprs = vec![];
                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg);
                        match r {
                            Ok(result) => {
                                if result.1 {
                                    return Ok(result);
                                } else {
                                    arg_exprs.push(*result.0);
                                }
                            }
                            Err(error) => {
                                return Err(error);
                            }
                        }
                    }

                    return Ok((
                        Box::new(ScalarFunction {
                            fun: function,
                            args: arg_exprs,
                        }),
                        false,
                    ));
                };

                if AggregateFunctionType::new(&name).is_ok() {
                    let arg = sql_function.args.first().unwrap();
                    let r = self.parse_sql_function_arg(arg)?;
                    return Ok((r.0, true));
                };

                bail!("Unsupported Expression: {:?}", expression)
            }
            _ => bail!("Unsupported Expression: {:?}", expression),
        }
    }

    fn parse_sql_function_arg(&self, argument: &FunctionArg) -> Result<(Box<Expression>, bool)> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(arg),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => bail!("Unsupported Wildcard argument: {:?}", argument),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => self.parse_sql_expression(arg),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                bail!("Unsupported Wildcard Argument: {:?}", argument)
            }
            _ => bail!("Unsupported Argument: {:?}", argument),
        }
    }

    fn parse_sql_unary_op(
        &self,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
    ) -> Result<(Box<Expression>, bool)> {
        let (arg, bypass) = self.parse_sql_expression(expr)?;
        if bypass {
            return Ok((arg, bypass));
        }

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => bail!("Unsupported SQL unary operator {:?}", op),
        };

        Ok((Box::new(Expression::UnaryOperator { operator, arg }), false))
    }

    fn parse_sql_binary_op(
        &self,
        left: &SqlExpr,
        op: &SqlBinaryOperator,
        right: &SqlExpr,
    ) -> Result<(Box<Expression>, bool)> {
        let (left_op, bypass_left) = self.parse_sql_expression(left)?;
        if bypass_left {
            return Ok((left_op, bypass_left));
        }
        let (right_op, bypass_right) = self.parse_sql_expression(right)?;
        if bypass_right {
            return Ok((right_op, bypass_right));
        }
        match op {
            SqlBinaryOperator::Plus => Ok((
                Box::new(Expression::BinaryOperator {
                    left: left_op,
                    operator: BinaryOperatorType::Add,
                    right: right_op,
                }),
                false,
            )),
            _ => bail!("Unsupported SQL binary operator {:?}", op),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<(Box<Expression>, bool)> {
        match n.parse::<i64>() {
            Ok(n) => Ok((Box::new(Expression::Literal(Field::Int(n))), false)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok((Box::new(Expression::Literal(Field::Float(f))), false)),
                Err(_) => bail!("Value is not Numeric."),
            },
        }
    }
}
