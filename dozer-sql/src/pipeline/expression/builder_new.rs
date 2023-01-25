#![allow(dead_code)]
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Schema},
};



use crate::pipeline::aggregation::aggregator::Aggregator;
use dozer_types::types::{FieldDefinition, SourceDefinition};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType, Expr as SqlExpr, Expr, Function, FunctionArg,
    FunctionArgExpr, Ident, TrimWhereField, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidExpression, InvalidNestedAggregationFunction, InvalidOperator,
    InvalidValue, TooManyArguments,
};
use crate::pipeline::errors::{PipelineError};

use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::ScalarFunction;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::expression::scalar::string::TrimType;

use super::cast::CastOperatorType;
pub struct ExpressionBuilder;

#[derive(Clone, PartialEq, Debug)]
pub struct AggregationMeasure {
    pub typ: Aggregator,
    pub arg: Expression,
}

impl AggregationMeasure {
    pub fn new(typ: Aggregator, arg: Expression) -> Self {
        Self { typ, arg }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ExpressionContext {
    pub aggrgeations: Vec<AggregationMeasure>,
}

impl ExpressionContext {
    pub fn new() -> Self {
        Self {
            aggrgeations: Vec::new(),
        }
    }
}

impl ExpressionBuilder {
    pub fn build(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        Self::parse_sql_expression(context, parse_aggregations, sql_expression, schema)
    }

    fn parse_sql_expression(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        match expression {
            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what,
            } => Self::parse_sql_trim_function(
                context,
                parse_aggregations,
                expr,
                trim_where,
                trim_what,
                schema,
            ),
            SqlExpr::Identifier(ident) => Self::parse_sql_column(&[ident.clone()], schema),
            SqlExpr::CompoundIdentifier(ident) => Self::parse_sql_column(ident, schema),
            SqlExpr::Value(SqlValue::Number(n, _)) => Self::parse_sql_number(n),
            SqlExpr::Value(SqlValue::Null) => Ok(Box::new(Expression::Literal(Field::Null))),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Self::parse_sql_string(s)
            }
            SqlExpr::UnaryOp { expr, op } => {
                Self::parse_sql_unary_op(context, parse_aggregations, op, expr, schema)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                Self::parse_sql_binary_op(context, parse_aggregations, left, op, right, schema)
            }
            SqlExpr::Nested(expr) => {
                Self::parse_sql_expression(context, parse_aggregations, expr, schema)
            }
            SqlExpr::Function(sql_function) => Self::parse_sql_function(
                context,
                parse_aggregations,
                sql_function,
                schema,
                expression,
            ),
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => Self::parse_sql_like_operator(
                context,
                parse_aggregations,
                negated,
                expr,
                pattern,
                escape_char,
                schema,
            ),
            SqlExpr::Cast { expr, data_type } => {
                Self::parse_sql_cast_operator(context, parse_aggregations, expr, data_type, schema)
            }
            _ => Err(InvalidExpression(format!("{:?}", expression))),
        }
    }

    fn parse_sql_column(
        ident: &[Ident],
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let (src_field, src_table_or_alias, src_connection) = match ident.len() {
            1 => (&ident[0].value, None, None),
            2 => (&ident[1].value, Some(&ident[0].value), None),
            3 => (
                &ident[2].value,
                Some(&ident[1].value),
                Some(&ident[0].value),
            ),
            _ => {
                return Err(InvalidExpression(
                    ident
                        .iter()
                        .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                ))
            }
        };

        let matching_by_field: Vec<(usize, &FieldDefinition)> = schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_idx, f)| &f.name == src_field)
            .collect();

        match matching_by_field.len() {
            1 => Ok(Box::new(Expression::Column {
                index: matching_by_field[0].0,
            })),
            _ => match src_table_or_alias {
                None => Err(InvalidExpression(
                    ident
                        .iter()
                        .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                )),
                Some(src_table_or_alias) => {
                    let matching_by_table_or_alias: Vec<(usize, &FieldDefinition)> =
                        matching_by_field
                            .into_iter()
                            .filter(|(_idx, field)| match &field.source {
                                SourceDefinition::Alias { name } => name == src_table_or_alias,
                                SourceDefinition::Table { name, connection: _ } => {
                                    name == src_table_or_alias
                                }
                                _ => false,
                            })
                            .collect();

                    match matching_by_table_or_alias.len() {
                        1 => Ok(Box::new(Expression::Column {
                            index: matching_by_table_or_alias[0].0,
                        })),
                        _ => match src_connection {
                            None => Err(InvalidExpression(
                                ident
                                    .iter()
                                    .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                            )),
                            Some(src_connection) => {
                                let matching_by_connection: Vec<(usize, &FieldDefinition)> =
                                    matching_by_table_or_alias
                                        .into_iter()
                                        .filter(|(_idx, field)| match &field.source {
                                            SourceDefinition::Table { name: _, connection } => {
                                                connection == src_connection
                                            }
                                            _ => false,
                                        })
                                        .collect();

                                match matching_by_connection.len() {
                                    1 => Ok(Box::new(Expression::Column {
                                        index: matching_by_connection[0].0,
                                    })),
                                    _ => Err(InvalidExpression(
                                        ident
                                            .iter()
                                            .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                                    )),
                                }
                            }
                        },
                    }
                }
            },
        }
    }

    fn parse_sql_trim_function(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        expr: &Expr,
        trim_where: &Option<TrimWhereField>,
        trim_what: &Option<Box<Expr>>,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let arg = Self::parse_sql_expression(context, parse_aggregations, expr, schema)?;
        let what = match trim_what {
            Some(e) => Some(Self::parse_sql_expression(
                context,
                parse_aggregations,
                e,
                schema,
            )?),
            _ => None,
        };
        let typ = trim_where.as_ref().map(|e| match e {
            TrimWhereField::Both => TrimType::Both,
            TrimWhereField::Leading => TrimType::Leading,
            TrimWhereField::Trailing => TrimType::Trailing,
        });
        Ok(Box::new(Expression::Trim { arg, what, typ }))
    }

    fn parse_sql_function(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        _expression: &SqlExpr,
    ) -> Result<Box<Expression>, PipelineError> {
        let function_name = sql_function.name.to_string().to_lowercase();

        match (
            Self::is_aggregation_function(function_name.as_str()),
            parse_aggregations,
        ) {
            (Some(aggr), true) => match sql_function.args.len() {
                1 => {
                    let measure = AggregationMeasure::new(
                        aggr,
                        *Self::parse_sql_function_arg(
                            context,
                            false,
                            &sql_function.args[0],
                            schema,
                        )?,
                    );
                    context.aggrgeations.push(measure);
                    Ok(Box::new(Expression::Column {
                        index: context.aggrgeations.len() - 1,
                    }))
                }
                _ => Err(TooManyArguments(function_name.clone())),
            },
            (Some(_agg), false) => Err(InvalidNestedAggregationFunction(function_name)),
            (None, _) => {
                let mut function_args: Vec<Expression> = Vec::new();
                for arg in &sql_function.args {
                    function_args.push(*Self::parse_sql_function_arg(
                        context,
                        parse_aggregations,
                        arg,
                        schema,
                    )?);
                }
                let function_type = ScalarFunctionType::new(function_name.as_str())?;

                Ok(Box::new(ScalarFunction {
                    fun: function_type,
                    args: function_args,
                }))
            }
        }
    }

    fn is_aggregation_function(name: &str) -> Option<Aggregator> {
        match name {
            "sum" => Some(Aggregator::Sum),
            "avg" => Some(Aggregator::Avg),
            "min" => Some(Aggregator::Min),
            "max" => Some(Aggregator::Max),
            "count" => Some(Aggregator::Count),
            _ => None,
        }
    }

    fn parse_sql_function_arg(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        argument: &FunctionArg,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => Self::parse_sql_expression(context, parse_aggregations, arg, schema),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Err(InvalidArgument(format!("{:?}", argument))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                Self::parse_sql_expression(context, parse_aggregations, arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Err(InvalidArgument(format!("{:?}", argument)))
            }
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::QualifiedWildcard(_),
            } => Err(InvalidArgument(format!("{:?}", argument))),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
                Err(InvalidArgument(format!("{:?}", argument)))
            }
        }
    }

    fn parse_sql_unary_op(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let arg = Self::parse_sql_expression(context, parse_aggregations, expr, schema)?;
        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => return Err(InvalidOperator(format!("{:?}", op))),
        };

        Ok(Box::new(Expression::UnaryOperator { operator, arg }))
    }

    fn parse_sql_binary_op(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        left: &SqlExpr,
        op: &SqlBinaryOperator,
        right: &SqlExpr,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let left_op = Self::parse_sql_expression(context, parse_aggregations, left, schema)?;
        let right_op = Self::parse_sql_expression(context, parse_aggregations, right, schema)?;

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
            _ => return Err(InvalidOperator(format!("{:?}", op))),
        };

        Ok(Box::new(Expression::BinaryOperator {
            left: left_op,
            operator,
            right: right_op,
        }))
    }

    fn parse_sql_number(n: &str) -> Result<Box<Expression>, PipelineError> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(Expression::Literal(Field::Int(n)))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(Expression::Literal(Field::Float(OrderedFloat(f))))),
                Err(_) => Err(InvalidValue(n.to_string())),
            },
        }
    }

    fn parse_sql_like_operator(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        negated: &bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let arg = Self::parse_sql_expression(context, parse_aggregations, expr, schema)?;
        let pattern = Self::parse_sql_expression(context, parse_aggregations, pattern, schema)?;
        let like_expression = Box::new(Expression::Like {
            arg,
            pattern,
            escape: *escape_char,
        });
        if *negated {
            Ok(Box::new(Expression::UnaryOperator {
                operator: UnaryOperatorType::Not,
                arg: like_expression,
            }))
        } else {
            Ok(like_expression)
        }
    }

    fn parse_sql_cast_operator(
        context: &mut ExpressionContext,
        parse_aggregations: bool,
        expr: &Expr,
        data_type: &DataType,
        schema: &Schema,
    ) -> Result<Box<Expression>, PipelineError> {
        let expression = Self::parse_sql_expression(context, parse_aggregations, expr, schema)?;
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
                        "Unsupported Cast type {}",
                        name
                    )))?
                }
            }
            _ => Err(PipelineError::InvalidFunction(format!(
                "Unsupported Cast type {}",
                data_type
            )))?,
        };
        Ok(Box::new(Expression::Cast {
            arg: expression,
            typ: cast_to,
        }))
    }

    fn parse_sql_string(s: &str) -> Result<Box<Expression>, PipelineError> {
        Ok(Box::new(Expression::Literal(Field::String(s.to_owned()))))
    }
}
