use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldDefinition, Schema, SourceDefinition},
};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType, Expr as SqlExpr, Expr, Function, FunctionArg,
    FunctionArgExpr, Ident, TrimWhereField, UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};

use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidExpression, InvalidNestedAggregationFunction, InvalidOperator,
    InvalidValue,
};
use crate::pipeline::errors::{PipelineError, SqlError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::datetime::DateTimeFunctionType;

use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::{
    DateTimeFunction, GeoFunction, ScalarFunction,
};
use crate::pipeline::expression::geo::common::GeoFunctionType;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::expression::scalar::string::TrimType;

use super::cast::CastOperatorType;

#[derive(Clone, PartialEq, Debug)]
pub struct ExpressionBuilder {
    // Must be an aggregation function
    pub aggregations: Vec<Expression>,
    pub offset: usize,
}

impl ExpressionBuilder {
    pub fn new(offset: usize) -> Self {
        Self {
            aggregations: Vec::new(),
            offset,
        }
    }

    pub fn from(offset: usize, aggregations: Vec<Expression>) -> Self {
        Self {
            aggregations,
            offset,
        }
    }

    pub fn build(
        &mut self,
        parse_aggregations: bool,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        self.parse_sql_expression(parse_aggregations, sql_expression, schema)
    }

    pub(crate) fn parse_sql_expression(
        &mut self,
        parse_aggregations: bool,
        expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        match expression {
            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what,
            } => self.parse_sql_trim_function(
                parse_aggregations,
                expr,
                trim_where,
                trim_what,
                schema,
            ),
            SqlExpr::Identifier(ident) => Self::parse_sql_column(&[ident.clone()], schema),
            SqlExpr::CompoundIdentifier(ident) => Self::parse_sql_column(ident, schema),
            SqlExpr::Value(SqlValue::Number(n, _)) => Self::parse_sql_number(n),
            SqlExpr::Value(SqlValue::Null) => Ok(Expression::Literal(Field::Null)),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Self::parse_sql_string(s)
            }
            SqlExpr::UnaryOp { expr, op } => {
                self.parse_sql_unary_op(parse_aggregations, op, expr, schema)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                self.parse_sql_binary_op(parse_aggregations, left, op, right, schema)
            }
            SqlExpr::Nested(expr) => self.parse_sql_expression(parse_aggregations, expr, schema),
            SqlExpr::Function(sql_function) => {
                self.parse_sql_function(parse_aggregations, sql_function, schema)
            }
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.parse_sql_like_operator(
                parse_aggregations,
                negated,
                expr,
                pattern,
                escape_char,
                schema,
            ),
            SqlExpr::Cast { expr, data_type } => {
                self.parse_sql_cast_operator(parse_aggregations, expr, data_type, schema)
            }
            _ => Err(InvalidExpression(format!("{expression:?}"))),
        }
    }

    fn parse_sql_column(ident: &[Ident], schema: &Schema) -> Result<Expression, PipelineError> {
        let (src_field, src_table_or_alias, src_connection) = match ident.len() {
            1 => (&ident[0].value, None, None),
            2 => (&ident[1].value, Some(&ident[0].value), None),
            3 => (
                &ident[2].value,
                Some(&ident[1].value),
                Some(&ident[0].value),
            ),
            _ => {
                return Err(PipelineError::SqlError(SqlError::InvalidColumn(
                    ident
                        .iter()
                        .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                )));
            }
        };

        let matching_by_field: Vec<(usize, &FieldDefinition)> = schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_idx, f)| &f.name == src_field)
            .collect();

        match matching_by_field.len() {
            1 => Ok(Expression::Column {
                index: matching_by_field[0].0,
            }),
            _ => match src_table_or_alias {
                None => Err(PipelineError::SqlError(SqlError::InvalidColumn(
                    ident
                        .iter()
                        .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                ))),
                Some(src_table_or_alias) => {
                    let matching_by_table_or_alias: Vec<(usize, &FieldDefinition)> =
                        matching_by_field
                            .into_iter()
                            .filter(|(_idx, field)| match &field.source {
                                SourceDefinition::Alias { name } => name == src_table_or_alias,
                                SourceDefinition::Table {
                                    name,
                                    connection: _,
                                } => name == src_table_or_alias,
                                _ => false,
                            })
                            .collect();

                    match matching_by_table_or_alias.len() {
                        1 => Ok(Expression::Column {
                            index: matching_by_table_or_alias[0].0,
                        }),
                        _ => match src_connection {
                            None => Err(PipelineError::SqlError(SqlError::InvalidColumn(
                                ident
                                    .iter()
                                    .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                            ))),
                            Some(src_connection) => {
                                let matching_by_connection: Vec<(usize, &FieldDefinition)> =
                                    matching_by_table_or_alias
                                        .into_iter()
                                        .filter(|(_idx, field)| match &field.source {
                                            SourceDefinition::Table {
                                                name: _,
                                                connection,
                                            } => connection == src_connection,
                                            _ => false,
                                        })
                                        .collect();

                                match matching_by_connection.len() {
                                    1 => Ok(Expression::Column {
                                        index: matching_by_connection[0].0,
                                    }),
                                    _ => Err(PipelineError::SqlError(SqlError::InvalidColumn(
                                        ident
                                            .iter()
                                            .fold(String::new(), |a, b| a + "." + b.value.as_str()),
                                    ))),
                                }
                            }
                        },
                    }
                }
            },
        }
    }

    fn parse_sql_trim_function(
        &mut self,
        parse_aggregations: bool,
        expr: &Expr,
        trim_where: &Option<TrimWhereField>,
        trim_what: &Option<Box<Expr>>,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let arg = Box::new(self.parse_sql_expression(parse_aggregations, expr, schema)?);
        let what = match trim_what {
            Some(e) => Some(Box::new(self.parse_sql_expression(
                parse_aggregations,
                e,
                schema,
            )?)),
            _ => None,
        };
        let typ = trim_where.as_ref().map(|e| match e {
            TrimWhereField::Both => TrimType::Both,
            TrimWhereField::Leading => TrimType::Leading,
            TrimWhereField::Trailing => TrimType::Trailing,
        });
        Ok(Expression::Trim { arg, what, typ })
    }

    fn parse_sql_function(
        &mut self,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let function_name = sql_function.name.to_string().to_lowercase();

        #[cfg(feature = "python")]
        if function_name.starts_with("py_") {
            // The function is from python udf.
            let udf_name = function_name.strip_prefix("py_").unwrap();
            return self.parse_python_udf(udf_name, sql_function, schema);
        }

        match (
            AggregateFunctionType::new(function_name.as_str()),
            parse_aggregations,
        ) {
            (Ok(aggr), true) => {
                let mut arg_expr: Vec<Expression> = Vec::new();
                for arg in &sql_function.args {
                    let aggregation = self.parse_sql_function_arg(true, arg, schema)?;
                    arg_expr.push(aggregation);
                }
                let measure = Expression::AggregateFunction {
                    fun: aggr,
                    args: arg_expr,
                };
                let index = match self
                    .aggregations
                    .iter()
                    .enumerate()
                    .find(|e| e.1 == &measure)
                {
                    Some((index, _existing)) => index,
                    _ => {
                        self.aggregations.push(measure);
                        self.aggregations.len() - 1
                    }
                };
                Ok(Expression::Column {
                    index: self.offset + index,
                })
            }
            (Ok(_agg), false) => Err(InvalidNestedAggregationFunction(function_name)),
            (Err(_), _) => {
                let mut function_args: Vec<Expression> = Vec::new();
                for arg in &sql_function.args {
                    function_args.push(self.parse_sql_function_arg(
                        parse_aggregations,
                        arg,
                        schema,
                    )?);
                }

                match ScalarFunctionType::new(function_name.as_str()) {
                    Ok(sft) => Ok(ScalarFunction {
                        fun: sft,
                        args: function_args.clone(),
                    }),
                    Err(_d) => match GeoFunctionType::new(function_name.as_str()) {
                        Ok(gft) => Ok(GeoFunction {
                            fun: gft,
                            args: function_args.clone(),
                        }),
                        Err(_e) => match DateTimeFunctionType::new(function_name.as_str()) {
                            Ok(dft) => {
                                let arg = function_args
                                    .first()
                                    .ok_or(InvalidArgument(function_name))
                                    .unwrap();
                                Ok(DateTimeFunction {
                                    fun: dft,
                                    arg: Box::new(arg.clone()),
                                })
                            }
                            Err(_err) => Err(InvalidNestedAggregationFunction(function_name)),
                        },
                    },
                }
            }
        }
    }

    fn parse_sql_function_arg(
        &mut self,
        parse_aggregations: bool,
        argument: &FunctionArg,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(parse_aggregations, arg, schema),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Err(InvalidArgument(format!("{argument:?}"))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(parse_aggregations, arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Err(InvalidArgument(format!("{argument:?}")))
            }
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::QualifiedWildcard(_),
            } => Err(InvalidArgument(format!("{argument:?}"))),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
                Err(InvalidArgument(format!("{argument:?}")))
            }
        }
    }

    fn parse_sql_unary_op(
        &mut self,
        parse_aggregations: bool,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let arg = Box::new(self.parse_sql_expression(parse_aggregations, expr, schema)?);
        let operator = match op {
            SqlUnaryOperator::Not => UnaryOperatorType::Not,
            SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
            SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
            _ => return Err(InvalidOperator(format!("{op:?}"))),
        };

        Ok(Expression::UnaryOperator { operator, arg })
    }

    fn parse_sql_binary_op(
        &mut self,
        parse_aggregations: bool,
        left: &SqlExpr,
        op: &SqlBinaryOperator,
        right: &SqlExpr,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let left_op = self.parse_sql_expression(parse_aggregations, left, schema)?;
        let right_op = self.parse_sql_expression(parse_aggregations, right, schema)?;

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
            _ => return Err(InvalidOperator(format!("{op:?}"))),
        };

        Ok(Expression::BinaryOperator {
            left: Box::new(left_op),
            operator,
            right: Box::new(right_op),
        })
    }

    fn parse_sql_number(n: &str) -> Result<Expression, PipelineError> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Expression::Literal(Field::Int(n))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Expression::Literal(Field::Float(OrderedFloat(f)))),
                Err(_) => Err(InvalidValue(n.to_string())),
            },
        }
    }

    fn parse_sql_like_operator(
        &mut self,
        parse_aggregations: bool,
        negated: &bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let arg = self.parse_sql_expression(parse_aggregations, expr, schema)?;
        let pattern = self.parse_sql_expression(parse_aggregations, pattern, schema)?;
        let like_expression = Expression::Like {
            arg: Box::new(arg),
            pattern: Box::new(pattern),
            escape: *escape_char,
        };
        if *negated {
            Ok(Expression::UnaryOperator {
                operator: UnaryOperatorType::Not,
                arg: Box::new(like_expression),
            })
        } else {
            Ok(like_expression)
        }
    }

    fn parse_sql_cast_operator(
        &mut self,
        parse_aggregations: bool,
        expr: &Expr,
        data_type: &DataType,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        let expression = self.parse_sql_expression(parse_aggregations, expr, schema)?;
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
        Ok(Expression::Cast {
            arg: Box::new(expression),
            typ: cast_to,
        })
    }

    fn parse_sql_string(s: &str) -> Result<Expression, PipelineError> {
        Ok(Expression::Literal(Field::String(s.to_owned())))
    }

    pub fn fullname_from_ident(ident: &[Ident]) -> String {
        let mut ident_tokens = vec![];
        for token in ident.iter() {
            ident_tokens.push(token.value.clone());
        }
        ident_tokens.join(".")
    }

    pub(crate) fn normalize_ident(id: &Ident) -> String {
        match id.quote_style {
            Some(_) => id.value.clone(),
            None => id.value.clone(),
        }
    }

    #[cfg(feature = "python")]
    fn parse_python_udf(
        &mut self,
        name: &str,
        function: &Function,
        schema: &Schema,
    ) -> Result<Expression, PipelineError> {
        // First, get python function define by name.
        // Then, transfer python function to Expression::PythonUDF

        use dozer_types::types::FieldType;
        use PipelineError::InvalidQuery;

        let args = function
            .args
            .iter()
            .map(|argument| self.parse_sql_function_arg(false, argument, schema))
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let last_arg = args
            .last()
            .ok_or_else(|| InvalidQuery("Can't get python udf return type".to_string()))?;

        let return_type = match last_arg {
            Expression::Literal(Field::String(s)) => {
                FieldType::try_from(s.as_str()).map_err(|e| InvalidQuery(format!("Failed to parse Python UDF return type: {e}")))?
            }
            _ => return Err(InvalidArgument("The last arg for python udf should be a string literal, which represents return type".to_string())),
        };

        Ok(Expression::PythonUDF {
            name: name.to_string(),
            args,
            return_type,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NameOrAlias(pub String, pub Option<String>);

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
