use std::collections::HashMap;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem, UnaryOperator as SqlUnaryOperator, Value as SqlValue};

use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_types::types::Field;
use dozer_types::types::Schema;

use crate::common::error;
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::expression::Expression;
use crate::pipeline::expression::expression::Expression::ScalarFunction;
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

    pub fn get_processor(
        &self,
        projection: &Vec<SelectItem>,
    ) -> error::Result<ProjectionProcessorFactory> {
        let expressions = projection
            .into_iter()
            .map(|expr| self.parse_sql_select_item(&expr))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Box<Expression>>>>()?;

        let names = projection
            .into_iter()
            .map(|item| self.get_select_item_name(&item))
            .collect::<Result<Vec<String>>>();


        Ok(ProjectionProcessorFactory::new(
            0,
            vec![DefaultPortHandle],
            vec![DefaultPortHandle],
            expressions,
            names.unwrap()
        ))
    }

    fn get_select_item_name(&self, item: &SelectItem) -> Result<String> {
        match item {
            SelectItem::UnnamedExpr(expr) => Ok(String::from(expr.to_string())),
            SelectItem::ExprWithAlias { expr: _, alias } => Ok(String::from(alias.to_string())),
            SelectItem::Wildcard => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Wildcard Operator"
            ))),
            SelectItem::QualifiedWildcard(ref object_name) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Qualified Wildcard Operator {}", object_name),
            )),
        }
    }

    fn parse_sql_select_item(&self, sql: &SelectItem) -> Result<Vec<Box<Expression>>> {
        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.parse_sql_expression(sql_expr) {
                    Ok(expr) => {
                        return Ok(vec![expr.0]);
                    }
                    Err(error) => return Err(error),
                };
            }
            SelectItem::ExprWithAlias { expr, alias } => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Expression {}:{}", expr, alias),
            )),
            SelectItem::Wildcard => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Wildcard"
            ))),
            SelectItem::QualifiedWildcard(ref object_name) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Qualified Wildcard {}", object_name),
            )),
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
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok((
                    Box::new(Expression::Literal(Field::String(s.clone()))),
                    false,
                ))
            },
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
                                    arg_exprs.push(result.0);
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

                if let Ok(_) = AggregateFunctionType::new(&name) {
                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg);
                        match r {
                            Ok(result) => {
                                return Ok((result.0, true));
                            }
                            Err(error) => {
                                return Err(error);
                            }
                        };
                    }
                };
                Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Expression: {:?}",
                    expression,
                )))
            }

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Expression: {:?}",
                expression,
            ))),
        }
    }

    fn parse_sql_function_arg(
        &self,
        argument: &FunctionArg,
    ) -> error::Result<(Box<Expression>, bool)> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(arg),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Wildcard argument: {:?}",
                argument
            ))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => self.parse_sql_expression(arg),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Wildcard Argument: {:?}", argument),
            )),
            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Argument: {:?}",
                argument
            ))),
        }
    }


    fn parse_sql_unary_op(&self, op: &SqlUnaryOperator, expr: &SqlExpr) -> Result<(Box<Expression>, bool)> {
        let (arg, bypass) = self.parse_sql_expression(expr)?;
        if bypass {
            return Ok((arg, bypass));
        }

        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => return Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL unary operator {:?}", op
            )))
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

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}",
                op
            ))),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<(Box<Expression>, bool)> {
        match n.parse::<i64>() {
            Ok(n) => Ok((Box::new(Expression::Literal(Field::Int(n))), false)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok((Box::new(Expression::Literal(Field::Float(f))), false)),
                Err(_) => Err(DozerSqlError::NotImplemented(format!(
                    "Value is not Numeric.",
                ))),
            },
        }
    }

}
