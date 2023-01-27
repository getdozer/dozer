use std::fmt::Display;

use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldDefinition, Schema, SourceDefinition},
};

use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType, Expr as SqlExpr, Expr, Function, FunctionArg,
    FunctionArgExpr, Ident, TrimWhereField, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::builder::PipelineError::InvalidArgument;
use crate::pipeline::expression::builder::PipelineError::InvalidExpression;
use crate::pipeline::expression::builder::PipelineError::InvalidOperator;
use crate::pipeline::expression::builder::PipelineError::InvalidValue;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::ScalarFunction;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::expression::scalar::string::TrimType;

use super::cast::CastOperatorType;

pub type Bypass = bool;

pub enum BuilderExpressionType {
    PreAggregation,
    Aggregation,
    // PostAggregation,
    FullExpression,
}
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NameOrAlias(pub String, pub Option<String>);

pub enum ConstraintIdentifier {
    Single(Ident),
    Compound(Vec<Ident>),
}

impl Display for ConstraintIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstraintIdentifier::Single(ident) => f.write_fmt(format_args!("{ident}")),
            ConstraintIdentifier::Compound(ident) => f.write_fmt(format_args!("{ident:?}")),
        }
    }
}
pub struct ExpressionBuilder;

impl ExpressionBuilder {
    pub fn build(
        &self,
        expression_type: &BuilderExpressionType,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let (expression, _bypass) =
            self.parse_sql_expression(expression_type, sql_expression, schema)?;
        Ok(expression)
    }

    pub fn parse_sql_expression(
        &self,
        expression_type: &BuilderExpressionType,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        match expression {
            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what,
            } => self.parse_sql_trim_function(expression_type, expr, trim_where, trim_what, schema),
            SqlExpr::Identifier(ident) => {
                let idx = get_field_index(&ConstraintIdentifier::Single(ident.clone()), schema);

                let idx = idx?.map_or(
                    Err(PipelineError::InvalidExpression(ident.value.to_string())),
                    Ok,
                )?;
                Ok((Box::new(Expression::Column { index: idx }), false))
            }
            SqlExpr::CompoundIdentifier(ident) => {
                let idx = get_field_index(&ConstraintIdentifier::Compound(ident.clone()), schema)?
                    .map_or(
                        Err(PipelineError::InvalidExpression(format!("{ident:?}"))),
                        Ok,
                    )?;
                Ok((Box::new(Expression::Column { index: idx }), false))
            }
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
                BuilderExpressionType::PreAggregation => self.parse_sql_function_pre_aggregation(
                    expression_type,
                    sql_function,
                    schema,
                    expression,
                ),
                BuilderExpressionType::Aggregation => self.parse_sql_function_aggregation(
                    expression_type,
                    sql_function,
                    schema,
                    expression,
                ),
                // ExpressionType::PostAggregation => todo!(),
                BuilderExpressionType::FullExpression => {
                    self.parse_sql_function(expression_type, sql_function, schema, expression)
                }
            },
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.parse_sql_like_operator(
                expression_type,
                negated,
                expr,
                pattern,
                escape_char,
                schema,
            ),
            SqlExpr::Cast { expr, data_type } => {
                self.parse_sql_cast_operator(expression_type, expr, data_type, schema)
            }
            _ => Err(InvalidExpression(format!("{expression:?}"))),
        }
    }

    fn parse_sql_trim_function(
        &self,
        expression_type: &BuilderExpressionType,
        expr: &Expr,
        trim_where: &Option<TrimWhereField>,
        trim_what: &Option<Box<Expr>>,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let arg = self.parse_sql_expression(expression_type, expr, schema)?.0;
        let what = match trim_what {
            Some(e) => Some(self.parse_sql_expression(expression_type, e, schema)?.0),
            _ => None,
        };
        let typ = trim_where.as_ref().map(|e| match e {
            TrimWhereField::Both => TrimType::Both,
            TrimWhereField::Leading => TrimType::Leading,
            TrimWhereField::Trailing => TrimType::Trailing,
        });
        Ok((Box::new(Expression::Trim { arg, what, typ }), false))
    }

    fn parse_sql_function(
        &self,
        expression_type: &BuilderExpressionType,
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
        Err(InvalidExpression(format!("{expression:?}")))
    }

    fn parse_sql_function_pre_aggregation(
        &self,
        expression_type: &BuilderExpressionType,
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
        Err(InvalidExpression(format!("{expression:?}")))
    }

    fn parse_sql_function_aggregation(
        &self,
        expression_type: &BuilderExpressionType,
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
            "Unsupported Expression: {expression:?}"
        )))
    }

    fn parse_sql_function_arg(
        &self,
        expression_type: &BuilderExpressionType,
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
            } => Err(InvalidArgument(format!("{argument:?}"))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(expression_type, arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Err(InvalidArgument(format!("{argument:?}")))
            }
            _ => Err(InvalidArgument(format!("{argument:?}"))),
        }
    }

    fn parse_sql_unary_op(
        &self,
        expression_type: &BuilderExpressionType,
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
            _ => return Err(InvalidOperator(format!("{op:?}"))),
        };

        Ok((Box::new(Expression::UnaryOperator { operator, arg }), false))
    }

    fn parse_sql_binary_op(
        &self,
        expression_type: &BuilderExpressionType,
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
            _ => return Err(InvalidOperator(format!("{op:?}"))),
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

    fn parse_sql_like_operator(
        &self,
        expression_type: &BuilderExpressionType,
        negated: &bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let arg = self.parse_sql_expression(expression_type, expr, schema)?;
        let pattern = self.parse_sql_expression(expression_type, pattern, schema)?;
        let like_expression = Box::new(Expression::Like {
            arg: arg.0,
            pattern: pattern.0,
            escape: *escape_char,
        });
        if *negated {
            Ok((
                Box::new(Expression::UnaryOperator {
                    operator: UnaryOperatorType::Not,
                    arg: like_expression,
                }),
                arg.1,
            ))
        } else {
            Ok((like_expression, arg.1))
        }
    }

    fn parse_sql_cast_operator(
        &self,
        expression_type: &BuilderExpressionType,
        expr: &Expr,
        data_type: &DataType,
        schema: &Schema,
    ) -> Result<(Box<Expression>, bool), PipelineError> {
        let expression = self.parse_sql_expression(expression_type, expr, schema)?;
        let cast_to = match data_type {
            DataType::Decimal(_) => CastOperatorType::Decimal,
            DataType::Binary(_) => CastOperatorType::Binary,
            DataType::Float(_) => CastOperatorType::Float,
            DataType::Int(_) => CastOperatorType::Int,
            DataType::Integer(_) => CastOperatorType::Int,
            DataType::UnsignedInt(_) => CastOperatorType::UInt,
            DataType::UnsignedInteger(_) => CastOperatorType::UInt,
            DataType::Boolean => CastOperatorType::Boolean,
            DataType::Date => CastOperatorType::Date,
            DataType::Timestamp(..) => CastOperatorType::Timestamp,
            DataType::Text => CastOperatorType::Text,
            DataType::String => CastOperatorType::String,
            DataType::Custom(name, ..) => {
                if name.to_string().to_lowercase() == "bson" {
                    CastOperatorType::Bson
                } else {
                    Err(PipelineError::InvalidFunction(format!(
                        "Unsupported Cast type {name}"
                    )))?
                }
            }
            _ => Err(PipelineError::InvalidFunction(format!(
                "Unsupported Cast type {data_type}"
            )))?,
        };
        Ok((
            Box::new(Expression::Cast {
                arg: expression.0,
                typ: cast_to,
            }),
            expression.1,
        ))
    }
}

pub fn fullname_from_ident(ident: &[Ident]) -> String {
    let mut ident_tokens = vec![];
    for token in ident.iter() {
        ident_tokens.push(token.value.clone());
    }
    ident_tokens.join(".")
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

pub fn extend_schema_source_def(schema: &Schema, name: &NameOrAlias) -> Schema {
    let mut output_schema = schema.clone();
    let mut fields = vec![];
    for mut field in schema.clone().fields.into_iter() {
        if let Some(alias) = &name.1 {
            field.source = SourceDefinition::Alias {
                name: alias.to_string(),
            };
        }

        fields.push(field);
    }
    output_schema.fields = fields;

    output_schema
}

pub fn get_field_index(
    ident: &ConstraintIdentifier,
    schema: &Schema,
) -> Result<Option<usize>, PipelineError> {
    let get_field_idx = |ident: &Ident, schema: &Schema| -> Option<(usize, FieldDefinition)> {
        schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == ident.value)
            .map(|(idx, fd)| (idx, fd.clone()))
    };

    let tables_matches = |table_ident: &Ident, fd: FieldDefinition| -> bool {
        match fd.source {
            dozer_types::types::SourceDefinition::Table {
                connection: _,
                name,
            } => name == table_ident.value,
            dozer_types::types::SourceDefinition::Alias { name } => name == table_ident.value,
            dozer_types::types::SourceDefinition::Dynamic => false,
        }
    };

    match ident {
        ConstraintIdentifier::Single(ident) => {
            let field_idx = get_field_idx(ident, schema);
            field_idx.map_or(
                Err(PipelineError::InvalidExpression(ident.value.to_string())),
                |t| Ok(Some(t.0)),
            )
        }
        ConstraintIdentifier::Compound(comp_ident) => {
            if comp_ident.len() > 2 {
                return Err(PipelineError::NameSpaceTooLong(
                    comp_ident
                        .iter()
                        .map(|a| a.value.clone())
                        .collect::<Vec<String>>()
                        .join("."),
                ));
            }
            let table_name = comp_ident.first().expect("table_name is expected");
            let field_name = comp_ident.last().expect("field_name is expected");

            let field_idx = get_field_idx(field_name, schema);
            if let Some((idx, fd)) = field_idx {
                if tables_matches(table_name, fd) {
                    return Ok(Some(idx));
                }
            }
            Ok(None)
        }
    }
}
