use crate::pipeline::errors::PipelineError::{
    self, InvalidArgument, InvalidExpression, InvalidOperator, InvalidValue,
};
use dozer_types::types::{Field, Schema};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::aggregation::sum::IntegerSumAggregator;
use crate::pipeline::expression::{
    aggregate::AggregateFunctionType,
    execution::Expression,
    operator::{BinaryOperatorType, UnaryOperatorType},
    scalar::ScalarFunctionType,
};

use crate::pipeline::expression::execution::Expression::ScalarFunction;

use super::aggregation::FieldRule;
use super::common::column_index;

// pub enum ExpressionVisitControl {
//     /// Continue to visit all the children
//     Continue(Box<Expression>),
//     /// Do not visit the children of this expression
//     Stop(Box<Expression>),
//     /// Bypass all the parents
//     Bypass(Box<Expression>),
// }

pub struct AggregationBuilder {}

impl AggregationBuilder {
    pub fn build(
        &self,
        select: &[SelectItem],
        groupby: &[SqlExpr],
        schema: &Schema,
    ) -> Result<Vec<FieldRule>, PipelineError> {
        let mut groupby_rules = groupby
            .iter()
            .map(|expr| self.get_groupby_field(expr, schema))
            .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

        let mut select_rules = select
            .iter()
            .map(|item| self.get_aggregation_field(item, schema))
            .filter(|e| e.is_ok())
            .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

        groupby_rules.append(&mut select_rules);

        Ok(groupby_rules)
    }

    pub fn get_groupby_field(
        &self,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<FieldRule, PipelineError> {
        match expression {
            SqlExpr::Identifier(ident) => Ok(FieldRule::Dimension(
                ident.value.clone(),
                Box::new(Expression::Column {
                    index: column_index(&ident.value, schema)?,
                }),
                true,
                None,
            )),
            _ => Err(InvalidExpression(format!(
                "Unsupported Group By Expression {:?}",
                expression
            ))),
        }
    }

    pub fn get_aggregation_field(
        &self,
        item: &SelectItem,
        schema: &Schema,
    ) -> Result<FieldRule, PipelineError> {
        match item {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.parse_sql_expression(sql_expr, schema) {
                    Ok(expr) => Ok(FieldRule::Measure(
                        sql_expr.to_string(),
                        self.get_aggregator(expr.0)?,
                        true,
                        Some(item.to_string()),
                    )),
                    Err(error) => Err(error),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => Err(InvalidExpression(format!(
                "Unsupported Expression {}:{}",
                expr, alias
            ))),
            SelectItem::Wildcard => Err(InvalidExpression(
                "Wildcard Operator is not supported".to_string(),
            )),
            SelectItem::QualifiedWildcard(ref _object_name) => Err(InvalidExpression(
                "Qualified Wildcard Operator is not supported".to_string(),
            )),
        }
    }

    fn get_aggregator(
        &self,
        expression: Box<Expression>,
    ) -> Result<Box<dyn Aggregator>, PipelineError> {
        match *expression {
            Expression::AggregateFunction { fun, args: _ } => match fun {
                AggregateFunctionType::Sum => Ok(Box::new(IntegerSumAggregator::new())),
                _ => Err(InvalidExpression(format!(
                    "Not implemented Aggreagation function: {:?}",
                    fun
                ))),
            },
            _ => Err(InvalidExpression(format!(
                "Not an Aggreagation function: {:?}",
                expression
            ))),
        }
    }

    fn parse_sql_expression(
        &self,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        match expression {
            SqlExpr::Identifier(ident) => Ok((
                Box::new(Expression::Column {
                    index: column_index(&ident.value, schema)?,
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
            SqlExpr::UnaryOp { expr, op } => Ok(self.parse_sql_unary_op(op, expr, schema)?),
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(left, op, right, schema)?)
            }
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr, schema)?),
            SqlExpr::Function(sql_function) => {
                let name = sql_function.name.to_string().to_lowercase();

                if let Ok(function) = ScalarFunctionType::new(&name) {
                    let mut arg_exprs = vec![];
                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg, schema);
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

                if let Ok(function) = AggregateFunctionType::new(&name) {
                    let mut arg_exprs = vec![];
                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg, schema);
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
                        Box::new(Expression::AggregateFunction {
                            fun: function,
                            args: arg_exprs,
                        }),
                        true, // switch bypass to true, since this Aggregation must be the final result
                    ));
                };

                Err(InvalidExpression(format!(
                    "Unsupported Expression: {:?}",
                    expression
                )))
            }
            _ => Err(InvalidExpression(format!(
                "Unsupported Expression: {:?}",
                expression
            ))),
        }
    }

    fn parse_sql_function_arg(
        &self,
        argument: &FunctionArg,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(arg, schema),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Err(InvalidArgument(format!(
                "Unsupported Wildcard Argument: {:?}",
                argument
            ))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(InvalidArgument(format!(
                "Unsupported Wildcard Argument: {:?}",
                argument
            ))),
            _ => Err(InvalidArgument(format!(
                "Unsupported Argument: {:?}",
                argument
            ))),
        }
    }

    fn parse_sql_unary_op(
        &self,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let (arg, bypass) = self.parse_sql_expression(expr, schema)?;
        if bypass {
            return Ok((arg, bypass));
        }

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => {
                return Err(InvalidOperator(format!(
                    "Unsupported SQL unary operator {:?}",
                    op
                )));
            }
        };

        Ok((Box::new(Expression::UnaryOperator { operator, arg }), false))
    }

    fn parse_sql_binary_op(
        &self,
        left: &SqlExpr,
        op: &SqlBinaryOperator,
        right: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let (left_op, bypass_left) = self.parse_sql_expression(left, schema)?;
        if bypass_left {
            return Ok((left_op, bypass_left));
        }
        let (right_op, bypass_right) = self.parse_sql_expression(right, schema)?;
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
            _ => Err(InvalidOperator(format!(
                "Unsupported SQL binary operator {:?}",
                op
            ))),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<(Box<Expression>, bool), PipelineError> {
        match n.parse::<i64>() {
            Ok(n) => Ok((Box::new(Expression::Literal(Field::Int(n))), false)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok((Box::new(Expression::Literal(Field::Float(f))), false)),
                Err(_) => Err(InvalidValue(n.to_string())),
            },
        }
    }
}
