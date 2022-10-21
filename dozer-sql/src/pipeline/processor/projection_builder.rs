use crate::pipeline::error::PipelineError;
use crate::pipeline::error::PipelineError::{
    InternalTypeError, InvalidArgument, InvalidExpression, InvalidOperator, InvalidValue,
};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::ScalarFunction;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use dozer_types::types::Schema;
use dozer_types::types::{Field, TypeError};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, SelectItem,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use std::collections::HashMap;

pub struct ProjectionBuilder {}

impl ProjectionBuilder {
    pub fn build_projection(
        &self,
        statement: &[SelectItem],
        schema: &Schema,
    ) -> Result<(Vec<Expression>, Vec<String>), PipelineError> {
        let expressions = statement
            .iter()
            .map(|expr| self.parse_sql_select_item(expr, schema))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expression>, PipelineError>>()?;

        let names = statement
            .iter()
            .map(|item| self.get_select_item_name(item))
            .collect::<Result<Vec<String>, PipelineError>>()?;

        Ok((expressions, names))
    }

    pub fn column_index(&self, name: &String, schema: &Schema) -> Result<usize, PipelineError> {
        let schema_idx: HashMap<String, usize> = schema
            .fields
            .iter()
            .enumerate()
            .map(|e| (e.1.name.clone(), e.0))
            .collect();

        if let Some(index) = schema_idx.get(name).cloned() {
            Ok(index)
        } else {
            Err(InternalTypeError(TypeError::InvalidFieldName(name.clone())))
        }
    }

    fn get_select_item_name(&self, item: &SelectItem) -> Result<String, PipelineError> {
        match item {
            SelectItem::UnnamedExpr(expr) => Ok(expr.to_string()),
            SelectItem::ExprWithAlias { expr: _, alias } => Ok(alias.to_string()),
            SelectItem::Wildcard => Err(InvalidOperator("*".to_string())),
            SelectItem::QualifiedWildcard(ref object_name) => {
                Err(InvalidOperator(object_name.to_string()))
            }
        }
    }

    fn parse_sql_select_item(
        &self,
        sql: &SelectItem,
        schema: &Schema,
    ) -> Result<Vec<Expression>, PipelineError> {
        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.parse_sql_expression(sql_expr, schema) {
                    Ok(expr) => Ok(vec![*expr.0]),
                    Err(error) => Err(error),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                Err(InvalidExpression(format!("{}:{}", expr, alias)))
            }
            SelectItem::Wildcard => Err(InvalidOperator("*".to_string())),
            SelectItem::QualifiedWildcard(ref object_name) => {
                Err(InvalidOperator(object_name.to_string()))
            }
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
                    index: self.column_index(&ident.value, schema)?,
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

                if AggregateFunctionType::new(&name).is_ok() {
                    let arg = sql_function.args.first().unwrap();
                    let r = self.parse_sql_function_arg(arg, schema)?;
                    return Ok((r.0, true));
                };

                Err(InvalidExpression(format!("{:?}", expression)))
            }
            _ => Err(InvalidExpression(format!("{:?}", expression))),
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
            } => Err(InvalidArgument(format!("{:?}", argument))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Err(InvalidArgument(format!("{:?}", argument)))
            }
            _ => Err(InvalidArgument(format!("{:?}", argument))),
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
            _ => return Err(InvalidOperator(format!("{:?}", op))),
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
            _ => Err(InvalidOperator(format!("{:?}", op))),
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
