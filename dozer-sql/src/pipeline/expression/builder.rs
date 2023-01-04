use std::cmp;

use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Schema},
};

use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr,
    Ident, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::PipelineError::InvalidArgument;
use crate::pipeline::expression::builder::PipelineError::InvalidExpression;
use crate::pipeline::expression::builder::PipelineError::InvalidOperator;
use crate::pipeline::expression::builder::PipelineError::InvalidValue;
use crate::pipeline::expression::execution::Expression::ScalarFunction;

use super::{
    aggregate::AggregateFunctionType,
    execution::Expression,
    operator::{BinaryOperatorType, UnaryOperatorType},
    scalar::ScalarFunctionType,
};

pub type Bypass = bool;

pub enum ExpressionType {
    PreAggregation,
    Aggregation,
    // PostAggregation,
    FullExpression,
}

pub struct ExpressionBuilder;

impl ExpressionBuilder {
    pub fn build(
        &self,
        expression_type: &ExpressionType,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let (expression, _bypass) =
            self.parse_sql_expression(expression_type, sql_expression, schema)?;
        Ok(expression)
    }

    pub fn parse_sql_expression(
        &self,
        expression_type: &ExpressionType,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        match expression {
            SqlExpr::Identifier(ident) => self.parse_sql_column(&[ident.clone()], schema),
            SqlExpr::CompoundIdentifier(ident) => self.parse_sql_column(ident, schema),
            SqlExpr::Value(SqlValue::Number(n, _)) => self.parse_sql_number(n),
            SqlExpr::Value(SqlValue::Null) => {
                Ok((Box::new(Expression::Literal(Field::Null)), false))
            }
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                parse_sql_string(s)
            }
            SqlExpr::UnaryOp { expr, op } => {
                self.parse_sql_unary_op(expression_type, op, expr, schema)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                self.parse_sql_binary_op(expression_type, left, op, right, schema)
            }
            SqlExpr::Nested(expr) => self.parse_sql_expression(expression_type, expr, schema),
            SqlExpr::Function(sql_function) => match expression_type {
                ExpressionType::PreAggregation => self.parse_sql_function_pre_aggregation(
                    expression_type,
                    sql_function,
                    schema,
                    expression,
                ),
                ExpressionType::Aggregation => self.parse_sql_function_aggregation(
                    expression_type,
                    sql_function,
                    schema,
                    expression,
                ),
                // ExpressionType::PostAggregation => todo!(),
                ExpressionType::FullExpression => {
                    self.parse_sql_function(expression_type, sql_function, schema, expression)
                }
            },
            _ => Err(InvalidExpression(format!("{:?}", expression))),
        }
    }

    fn parse_sql_column(
        &self,
        ident: &[Ident],
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        Ok((
            Box::new(Expression::Column {
                index: get_field_index(ident, schema)?,
                //index: schema.get_field_index(&ident[0].value)?.0,
            }),
            false,
        ))
    }

    fn parse_sql_function(
        &self,
        expression_type: &ExpressionType,
        sql_function: &Function,
        schema: &Schema,
        expression: &SqlExpr,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let name = sql_function.name.to_string().to_lowercase();
        if let Ok(function) = ScalarFunctionType::new(&name) {
            let mut arg_exprs = vec![];
            for arg in &sql_function.args {
                let r = self.parse_sql_function_arg(expression_type, arg, schema);
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
            let r = self.parse_sql_function_arg(expression_type, arg, schema)?;
            return Ok((r.0, false)); // switch bypass to true, since the argument of this Aggregation must be the final result
        };
        Err(InvalidExpression(format!("{:?}", expression)))
    }

    fn parse_sql_function_pre_aggregation(
        &self,
        expression_type: &ExpressionType,
        sql_function: &Function,
        schema: &Schema,
        expression: &SqlExpr,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let name = sql_function.name.to_string().to_lowercase();

        if let Ok(function) = ScalarFunctionType::new(&name) {
            let mut arg_exprs = vec![];
            for arg in &sql_function.args {
                let r = self.parse_sql_function_arg(expression_type, arg, schema);
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
            let r = self.parse_sql_function_arg(expression_type, arg, schema)?;
            return Ok((r.0, true)); // switch bypass to true, since the argument of this Aggregation must be the final result
        };
        Err(InvalidExpression(format!("{:?}", expression)))
    }

    fn parse_sql_function_aggregation(
        &self,
        expression_type: &ExpressionType,
        sql_function: &Function,
        schema: &Schema,
        expression: &SqlExpr,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let name = sql_function.name.to_string().to_lowercase();

        if let Ok(function) = ScalarFunctionType::new(&name) {
            let mut arg_exprs = vec![];
            for arg in &sql_function.args {
                let r = self.parse_sql_function_arg(expression_type, arg, schema);
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
                let r = self.parse_sql_function_arg(expression_type, arg, schema);
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

    fn parse_sql_function_arg(
        &self,
        expression_type: &ExpressionType,
        argument: &FunctionArg,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(expression_type, arg, schema),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Err(InvalidArgument(format!("{:?}", argument))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(expression_type, arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Err(InvalidArgument(format!("{:?}", argument)))
            }
            _ => Err(InvalidArgument(format!("{:?}", argument))),
        }
    }

    fn parse_sql_unary_op(
        &self,
        expression_type: &ExpressionType,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, Bypass), PipelineError> {
        let (arg, bypass) = self.parse_sql_expression(expression_type, expr, schema)?;
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
        expression_type: &ExpressionType,
        left: &SqlExpr,
        op: &SqlBinaryOperator,
        right: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let (left_op, bypass_left) = self.parse_sql_expression(expression_type, left, schema)?;
        if bypass_left {
            return Ok((left_op, bypass_left));
        }
        let (right_op, bypass_right) = self.parse_sql_expression(expression_type, right, schema)?;
        if bypass_right {
            return Ok((right_op, bypass_right));
        }

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
                left: left_op,
                operator,
                right: right_op,
            }),
            false,
        ))
    }

    fn parse_sql_number(&self, n: &str) -> Result<(Box<Expression>, Bypass), PipelineError> {
        match n.parse::<i64>() {
            Ok(n) => Ok((Box::new(Expression::Literal(Field::Int(n))), false)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok((
                    Box::new(Expression::Literal(Field::Float(OrderedFloat(f)))),
                    false,
                )),
                Err(_) => Err(InvalidValue(n.to_string())),
            },
        }
    }
}

pub fn fullname_from_ident(ident: &[Ident]) -> String {
    let mut ident_tokens = vec![];
    for token in ident.iter() {
        ident_tokens.push(token.value.clone());
    }
    ident_tokens.join(".")
}

pub fn get_field_index(ident: &[Ident], schema: &Schema) -> Result<usize, PipelineError> {
    let full_ident = fullname_from_ident(ident);

    let mut field_index: Option<usize> = None;

    for (index, field) in schema.fields.iter().enumerate() {
        if compare_name(field.name.clone(), full_ident.clone()) {
            if field_index.is_some() {
                return Err(PipelineError::InvalidQuery(format!(
                    "Ambiguous Field {}",
                    full_ident
                )));
            } else {
                field_index = Some(index);
            }
        }
    }
    if let Some(index) = field_index {
        Ok(index)
    } else {
        return Err(PipelineError::InvalidQuery(format!(
            "Field {} not found",
            full_ident
        )));
    }
}

pub(crate) fn compare_name(name: String, ident: String) -> bool {
    let left = name.split('.').collect::<Vec<&str>>();
    let right = ident.split('.').collect::<Vec<&str>>();

    let left_len = left.len();
    let right_len = right.len();

    let shorter = cmp::min(left_len, right_len);
    let mut is_equal = false;
    for i in 1..shorter + 1 {
        if left[left_len - i] == right[right_len - i] {
            is_equal = true;
        } else {
            is_equal = false;
            break;
        }
    }

    is_equal
}

fn parse_sql_string(s: &str) -> Result<(Box<Expression>, bool), PipelineError> {
    Ok((
        Box::new(Expression::Literal(Field::String(s.to_owned()))),
        false,
    ))
}

pub(crate) fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.clone(),
    }
}
