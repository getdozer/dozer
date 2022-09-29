use std::collections::HashMap;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem, Value as SqlValue};
use std::sync::Arc;
use crate::common::error;
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::expression::Expression;
use crate::pipeline::expression::expression::Expression::{AggregateFunction, ScalarFunction};
use crate::pipeline::expression::operator::OperatorType;
use crate::pipeline::expression::scalar::ScalarFunctionType;
use crate::pipeline::processor::projection::ProjectionProcessor;
use dozer_types::types::Schema;
use dozer_core::dag::node::Processor;
use dozer_types::types::Field;

pub struct ProjectionBuilder {
    schema_idx: HashMap<String, usize>,
    expression_builder: ExpressionBuilder,
}

impl ProjectionBuilder {

    pub fn new(schema: &Schema) -> ProjectionBuilder {
        Self {
            schema_idx: schema.fields.iter().enumerate().map(|e| (e.1.name.clone(), e.0)).collect(),
            expression_builder: ExpressionBuilder::new(schema.clone())
        }
    }

    pub fn get_processor(&self, projection: Vec<SelectItem>) -> error::Result<Arc<dyn Processor>> {
        let expressions = projection.into_iter()
            .map(|expr| {
                self.parse_sql_select_item(&expr)
            })
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Box<Expression>>>>()?;
        Ok(Arc::new(ProjectionProcessor::new(0, None, None, expressions)))
    }

    fn parse_sql_select_item(
        &self,
        sql: &SelectItem,
    ) -> Result<Vec<Box<Expression>>> {

        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.parse_sql_expression(sql_expr) {

                    Ok(expr) => {
                        let e = &expr;
                        return Ok(vec![expr.0])
                    },
                    Err(error) => return Err(error),
                };
            },
            SelectItem::ExprWithAlias { expr, alias } => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Expression {}", expr)
            )),
            SelectItem::Wildcard => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Wildcard")
            )),
            SelectItem::QualifiedWildcard(ref object_name) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Qualified Wildcard {}", object_name)
            ))
        }
    }

    fn parse_sql_expression(&self, expression: &SqlExpr) -> Result<(Box<Expression>, bool)> {

        match expression {
            SqlExpr::Identifier(ident) => {
               Ok((Box::new(
                   Expression::Column{ index: *self.schema_idx.get(&ident.value).unwrap()}),
                   false))
            },
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) |
                           SqlValue::DoubleQuotedString(s)) => Ok((Box::new(Expression::Literal(Field::String(s.clone()))), false)),
            SqlExpr::BinaryOp { left, op, right } => Ok(self.parse_sql_binary_op(left, op, right)?),
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr)?),
            SqlExpr::Function(sql_function) => {
                let name = sql_function.name.to_string().to_lowercase();

                if let Ok(function) = ScalarFunctionType::new(&name) {
                    let f = &function;
                    let mut arg_exprs = vec![];
                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg);
                        match r {
                            Ok(result) => {
                                if result.1 {
                                    return Ok(result);
                                }
                                else {
                                    arg_exprs.push(result.0);
                                }
                            },
                            Err(error) => {
                                return Err(error);
                            }
                        }

                    }

                    println!("here");
                    return Ok((Box::new(ScalarFunction { fun: function, args: arg_exprs }), false));
                };

                if let Ok(function) = AggregateFunctionType::new(&name) {
                    let f = &function;

                    for arg in &sql_function.args {
                        let r = self.parse_sql_function_arg(arg);
                        match r {
                            Ok(result) => {
                                return Ok((result.0, true));
                            },
                            Err(error) => {
                                return Err(error);
                            }
                        };

                    }

                };
                Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Expression: {:?}", expression,
                )))
            }

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Expression: {:?}", expression,
            ))),
        }
    }

    fn parse_sql_function_arg(&self, argument: &FunctionArg) -> error::Result<(Box<Expression>, bool)> {
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
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                    self.parse_sql_expression(arg)
                }
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Wildcard Argument: {:?}",
                    argument
                ))),
                _ => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Argument: {:?}",
                    argument
                )))
            }
        }


        fn parse_sql_binary_op(&self,
                           left: &SqlExpr,
                           op: &BinaryOperator,
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

            BinaryOperator::Plus => Ok((Box::new(Expression::Binary {
                left:left_op,
                operator: OperatorType::Sum,
                right:right_op}), false)),

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}", op
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
