use dozer_types::models::udf_config::UdfConfig;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldDefinition, Schema, SourceDefinition},
};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType, DateTimeField, Expr as SqlExpr, Expr, Function,
    FunctionArg, FunctionArgExpr, Ident, Interval, TrimWhereField,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use crate::pipeline::errors::PipelineError::{InvalidArgument, InvalidExpression, InvalidFunction, InvalidNestedAggregationFunction, InvalidOperator, InvalidValue};
use crate::pipeline::errors::{PipelineError, SqlError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::conditional::ConditionalExpressionType;
use crate::pipeline::expression::datetime::DateTimeFunctionType;

use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::{
    ConditionalExpression, GeoFunction, Now, ScalarFunction,
};
use crate::pipeline::expression::geo::common::GeoFunctionType;
use crate::pipeline::expression::json_functions::JsonFunctionType;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::expression::scalar::string::TrimType;

use super::cast::CastOperatorType;

#[cfg(feature = "onnx")]
use dozer_types::models::udf_config::OnnxConfig;
#[cfg(feature = "onnx")]
use dozer_types::models::udf_config::UdfType::Onnx;
#[cfg(feature = "onnx")]
use crate::pipeline::onnx::DozerSession;
#[cfg(feature = "onnx")]
use crate::pipeline::onnx::OnnxError::OnnxOrtErr;
#[cfg(feature = "onnx")]
use crate::pipeline::errors::PipelineError::OnnxError;

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
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        self.parse_sql_expression(parse_aggregations, sql_expression, schema, udfs)
    }

    pub(crate) fn parse_sql_expression(
        &mut self,
        parse_aggregations: bool,
        expression: &SqlExpr,
        schema: &Schema,
        udfs: &[UdfConfig],
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
                udfs,
            ),
            SqlExpr::Identifier(ident) => Self::parse_sql_column(&[ident.clone()], schema),
            SqlExpr::CompoundIdentifier(ident) => Self::parse_sql_column(ident, schema),
            SqlExpr::Value(SqlValue::Number(n, _)) => Self::parse_sql_number(n),
            SqlExpr::Value(SqlValue::Null) => Ok(Expression::Literal(Field::Null)),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Self::parse_sql_string(s)
            }
            SqlExpr::UnaryOp { expr, op } => {
                self.parse_sql_unary_op(parse_aggregations, op, expr, schema, udfs)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                self.parse_sql_binary_op(parse_aggregations, left, op, right, schema, udfs)
            }
            SqlExpr::Nested(expr) => {
                self.parse_sql_expression(parse_aggregations, expr, schema, udfs)
            }
            SqlExpr::Function(sql_function) => {
                self.parse_sql_function(parse_aggregations, sql_function, schema, udfs)
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
                udfs,
            ),
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => self.parse_sql_in_list_operator(
                parse_aggregations,
                expr,
                list,
                *negated,
                schema,
                udfs,
            ),

            SqlExpr::Cast { expr, data_type } => {
                self.parse_sql_cast_operator(parse_aggregations, expr, data_type, schema, udfs)
            }
            SqlExpr::Extract { field, expr } => {
                self.parse_sql_extract_operator(parse_aggregations, field, expr, schema, udfs)
            }
            SqlExpr::Interval(Interval {
                value,
                leading_field,
                leading_precision: _,
                last_field: _,
                fractional_seconds_precision: _,
            }) => self.parse_sql_interval_expression(
                parse_aggregations,
                value,
                leading_field,
                schema,
                udfs,
            ),
            SqlExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.parse_sql_case_expression(
                parse_aggregations,
                operand,
                conditions,
                results,
                else_result,
                schema,
                udfs,
            ),
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
                        .map(|e| e.value.as_str())
                        .collect::<Vec<&str>>()
                        .join("."),
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
                        .map(|e| e.value.as_str())
                        .collect::<Vec<&str>>()
                        .join("."),
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
                                            .map(|e| e.value.as_str())
                                            .collect::<Vec<&str>>()
                                            .join("."),
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
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let arg = Box::new(self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?);
        let what = match trim_what {
            Some(e) => Some(Box::new(self.parse_sql_expression(
                parse_aggregations,
                e,
                schema,
                udfs,
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

    fn aggr_function_check(
        &mut self,
        function_name: String,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        match (
            AggregateFunctionType::new(function_name.as_str()),
            parse_aggregations,
        ) {
            (Ok(aggr), true) => {
                let mut arg_expr: Vec<Expression> = Vec::new();
                for arg in &sql_function.args {
                    let aggregation = self.parse_sql_function_arg(true, arg, schema, udfs)?;
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
            (Err(_), _) => Err(InvalidNestedAggregationFunction(function_name)),
        }
    }

    fn scalar_function_check(
        &mut self,
        function_name: String,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let mut function_args: Vec<Expression> = Vec::new();
        for arg in &sql_function.args {
            function_args.push(self.parse_sql_function_arg(
                parse_aggregations,
                arg,
                schema,
                udfs,
            )?);
        }

        match ScalarFunctionType::new(function_name.as_str()) {
            Ok(sft) => Ok(ScalarFunction {
                fun: sft,
                args: function_args.clone(),
            }),
            Err(_d) => Err(InvalidFunction(function_name)),
        }
    }

    fn geo_expr_check(
        &mut self,
        function_name: String,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let mut function_args: Vec<Expression> = Vec::new();
        for arg in &sql_function.args {
            function_args.push(self.parse_sql_function_arg(
                parse_aggregations,
                arg,
                schema,
                udfs,
            )?);
        }

        match GeoFunctionType::new(function_name.as_str()) {
            Ok(gft) => Ok(GeoFunction {
                fun: gft,
                args: function_args.clone(),
            }),
            Err(_e) => Err(InvalidFunction(function_name)),
        }
    }

    fn datetime_expr_check(&mut self, function_name: String) -> Result<Expression, PipelineError> {
        match DateTimeFunctionType::new(function_name.as_str()) {
            Ok(dtf) => Ok(Now { fun: dtf }),
            Err(_e) => Err(InvalidFunction(function_name)),
        }
    }

    fn json_func_check(
        &mut self,
        function_name: String,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let mut function_args: Vec<Expression> = Vec::new();
        for arg in &sql_function.args {
            function_args.push(self.parse_sql_function_arg(
                parse_aggregations,
                arg,
                schema,
                udfs,
            )?);
        }

        match JsonFunctionType::new(function_name.as_str()) {
            Ok(jft) => Ok(Expression::Json {
                fun: jft,
                args: function_args,
            }),
            Err(_e) => Err(InvalidFunction(function_name)),
        }
    }

    fn conditional_expr_check(
        &mut self,
        function_name: String,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let mut function_args: Vec<Expression> = Vec::new();
        for arg in &sql_function.args {
            function_args.push(self.parse_sql_function_arg(
                parse_aggregations,
                arg,
                schema,
                udfs,
            )?);
        }

        match ConditionalExpressionType::new(function_name.as_str()) {
            Ok(cet) => Ok(ConditionalExpression {
                fun: cet,
                args: function_args.clone(),
            }),
            Err(_err) => Err(InvalidFunction(function_name)),
        }
    }

    fn parse_sql_function(
        &mut self,
        parse_aggregations: bool,
        sql_function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let function_name = sql_function.name.to_string().to_lowercase();

        #[cfg(feature = "python")]
        if function_name.starts_with("py_") {
            // The function is from python udf.
            let udf_name = function_name.strip_prefix("py_").unwrap();
            return self.parse_python_udf(udf_name, sql_function, schema, &udfs.to_vec());
        }

        let aggr_check = self.aggr_function_check(
            function_name.clone(),
            parse_aggregations,
            sql_function,
            schema,
            udfs.clone(),
        );
        if aggr_check.is_ok() {
            return aggr_check;
        }

        let scalar_check = self.scalar_function_check(
            function_name.clone(),
            parse_aggregations,
            sql_function,
            schema,
            udfs.clone(),
        );
        if scalar_check.is_ok() {
            return scalar_check;
        }

        let geo_check = self.geo_expr_check(
            function_name.clone(),
            parse_aggregations,
            sql_function,
            schema,
            udfs.clone(),
        );
        if geo_check.is_ok() {
            return geo_check;
        }

        let conditional_check = self.conditional_expr_check(
            function_name.clone(),
            parse_aggregations,
            sql_function,
            schema,
            udfs.clone(),
        );
        if conditional_check.is_ok() {
            return conditional_check;
        }

        let datetime_check = self.datetime_expr_check(function_name.clone());
        if datetime_check.is_ok() {
            return datetime_check;
        }

        let json_check = self.json_func_check(
            function_name.clone(),
            parse_aggregations,
            sql_function,
            schema,
            udfs.clone(),
        );
        if json_check.is_ok() {
            return json_check;
        }

        // config check for udfs
        let udf_type = udfs.iter().find(|udf| udf.name == function_name).ok_or(PipelineError::UdfConfigMissing(function_name.clone()))?;
        return match &udf_type.config {
            #[cfg(feature = "onnx")]
            Some(Onnx(config)) => self.parse_onnx_udf(
                function_name.clone(),
                &config,
                sql_function,
                schema,
                udfs,
            ),
            None => Err(PipelineError::UdfConfigMissing(function_name.clone())),
        }
    }

    fn parse_sql_function_arg(
        &mut self,
        parse_aggregations: bool,
        argument: &FunctionArg,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        match argument {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.parse_sql_expression(parse_aggregations, arg, schema, udfs),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Ok(Expression::Literal(Field::Null)),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.parse_sql_expression(parse_aggregations, arg, schema, udfs)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expression::Literal(Field::Null)),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::QualifiedWildcard(_),
            } => Err(InvalidArgument(format!("{argument:?}"))),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
                Err(InvalidArgument(format!("{argument:?}")))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_sql_case_expression(
        &mut self,
        parse_aggregations: bool,
        operand: &Option<Box<Expr>>,
        conditions: &[Expr],
        results: &[Expr],
        else_result: &Option<Box<Expr>>,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let op = match operand {
            Some(o) => Some(Box::new(self.parse_sql_expression(
                parse_aggregations,
                o,
                schema,
                udfs,
            )?)),
            None => None,
        };
        let conds = conditions
            .iter()
            .map(|cond| self.parse_sql_expression(parse_aggregations, cond, schema, udfs))
            .collect::<Result<Vec<_>, PipelineError>>()?;
        let res = results
            .iter()
            .map(|r| self.parse_sql_expression(parse_aggregations, r, schema, udfs))
            .collect::<Result<Vec<_>, PipelineError>>()?;
        let else_res = match else_result {
            Some(r) => Some(Box::new(self.parse_sql_expression(
                parse_aggregations,
                r,
                schema,
                udfs,
            )?)),
            None => None,
        };

        Ok(Expression::Case {
            operand: op,
            conditions: conds,
            results: res,
            else_result: else_res,
        })
    }

    fn parse_sql_interval_expression(
        &mut self,
        parse_aggregations: bool,
        value: &Expr,
        leading_field: &Option<DateTimeField>,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let right = self.parse_sql_expression(parse_aggregations, value, schema, udfs)?;
        if leading_field.is_some() {
            Ok(Expression::DateTimeFunction {
                fun: DateTimeFunctionType::Interval {
                    field: leading_field.unwrap(),
                },
                arg: Box::new(right),
            })
        } else {
            Err(InvalidExpression(format!("INTERVAL for {leading_field:?}")))
        }
    }

    fn parse_sql_unary_op(
        &mut self,
        parse_aggregations: bool,
        op: &SqlUnaryOperator,
        expr: &SqlExpr,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let arg = Box::new(self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?);
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
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let left_op = self.parse_sql_expression(parse_aggregations, left, schema, udfs)?;
        let right_op = self.parse_sql_expression(parse_aggregations, right, schema, udfs)?;

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

    #[cfg(not(feature = "bigdecimal"))]
    fn parse_sql_number(n: &str) -> Result<Expression, PipelineError> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Expression::Literal(Field::Int(n))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Expression::Literal(Field::Float(OrderedFloat(f)))),
                Err(_) => Err(InvalidValue(n.to_string())),
            },
        }
    }

    #[cfg(feature = "bigdecimal")]
    fn parse_sql_number(n: &bigdecimal::BigDecimal) -> Result<Expression, PipelineError> {
        use bigdecimal::ToPrimitive;
        if n.is_integer() {
            Ok(Expression::Literal(Field::Int(n.to_i64().unwrap())))
        } else {
            match n.to_f64() {
                Some(f) => Ok(Expression::Literal(Field::Float(OrderedFloat(f)))),
                None => Err(InvalidValue(n.to_string())),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_sql_like_operator(
        &mut self,
        parse_aggregations: bool,
        negated: &bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let arg = self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?;
        let pattern = self.parse_sql_expression(parse_aggregations, pattern, schema, udfs)?;
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

    fn parse_sql_extract_operator(
        &mut self,
        parse_aggregations: bool,
        field: &sqlparser::ast::DateTimeField,
        expr: &Expr,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let right = self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?;
        Ok(Expression::DateTimeFunction {
            fun: DateTimeFunctionType::Extract { field: *field },
            arg: Box::new(right),
        })
    }

    fn parse_sql_cast_operator(
        &mut self,
        parse_aggregations: bool,
        expr: &Expr,
        data_type: &DataType,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let expression = self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?;
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
            DataType::JSON => CastOperatorType::Json,
            DataType::Custom(name, ..) => {
                if name.to_string().to_lowercase() == "uint" {
                    CastOperatorType::UInt
                } else if name.to_string().to_lowercase() == "u128" {
                    CastOperatorType::U128
                } else if name.to_string().to_lowercase() == "i128" {
                    CastOperatorType::I128
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
        udfs: &Vec<UdfConfig>,
    ) -> Result<Expression, PipelineError> {
        // First, get python function define by name.
        // Then, transfer python function to Expression::PythonUDF
        use PipelineError::InvalidQuery;
        use dozer_types::types::FieldType;

        let args = function
            .args
            .iter()
            .map(|argument| self.parse_sql_function_arg(false, argument, schema, udfs))
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let return_type = {
            let ident = function
                .return_type
                .as_ref()
                .ok_or_else(|| InvalidQuery("Python UDF must have a return type. The syntax is: function_name<return_type>(arguments)".to_string()))?;

            FieldType::try_from(ident.value.as_str())
                .map_err(|e| InvalidQuery(format!("Failed to parse Python UDF return type: {e}")))?
        };

        Ok(Expression::PythonUDF {
            name: name.to_string(),
            args,
            return_type,
        })
    }

    #[cfg(feature = "onnx")]
    fn parse_onnx_udf(
        &mut self,
        name: String,
        config: &OnnxConfig,
        function: &Function,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        // First, get onnx function define by name.
        // Then, transfer onnx function to Expression::OnnxUDF
        use ort::{Environment, GraphOptimizationLevel, LoggingLevel, SessionBuilder};
        use std::path::Path;
        use crate::pipeline::expression::onnx::onnx_utils::{onnx_input_validation, onnx_output_validation};

        let args = function
            .args
            .iter()
            .map(|argument| self.parse_sql_function_arg(false, argument, schema, udfs))
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let environment = Environment::builder()
            .with_name("dozer_onnx")
            .with_log_level(LoggingLevel::Verbose)
            .build()
            .map_err(|e| OnnxError(OnnxOrtErr(e)))?
            .into_arc();

        let session = SessionBuilder::new(&environment)
            .map_err(|e| OnnxError(OnnxOrtErr(e)))?
            .with_optimization_level(GraphOptimizationLevel::Level1)
            .map_err(|e| OnnxError(OnnxOrtErr(e)))?
            .with_intra_threads(1)
            .map_err(|e| OnnxError(OnnxOrtErr(e)))?
            .with_model_from_file(Path::new(config.path.as_str()))
            .map_err(|e| OnnxError(OnnxOrtErr(e)))?;

        // input number, type, shape validation
        onnx_input_validation(schema, &args, &session.inputs)?;
        // output number, type, shape validation
        onnx_output_validation(&session.outputs)?;

        Ok(Expression::OnnxUDF {
            name: name.to_string(),
            session: DozerSession(session.into()),
            args,
        })
    }

    fn parse_sql_in_list_operator(
        &mut self,
        parse_aggregations: bool,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        schema: &Schema,
        udfs: &[UdfConfig],
    ) -> Result<Expression, PipelineError> {
        let expr = self.parse_sql_expression(parse_aggregations, expr, schema, udfs)?;
        let list = list
            .iter()
            .map(|expr| self.parse_sql_expression(parse_aggregations, expr, schema, udfs))
            .collect::<Result<Vec<_>, PipelineError>>()?;
        let in_list_expression = Expression::InList {
            expr: Box::new(expr),
            list,
            negated,
        };

        Ok(in_list_expression)
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
