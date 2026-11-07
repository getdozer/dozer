use std::sync::Arc;

use crate::aggregate::AggregateFunctionType;
use crate::conditional::ConditionalExpressionType;
use crate::datetime::DateTimeFunctionType;
use crate::error::Error;
use dozer_types::models::udf_config::{UdfConfig, UdfType};
use dozer_types::types::FieldType;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldDefinition, Schema, SourceDefinition},
};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DataType, DateTimeField, Expr as SqlExpr, Expr, Function,
    FunctionArg, FunctionArgExpr, Ident, Interval, TrimWhereField,
    UnaryOperator as SqlUnaryOperator, Value as SqlValue,
};
use tokio::runtime::Runtime;

use crate::execution::Expression;
use crate::execution::Expression::{ConditionalExpression, GeoFunction, Now, ScalarFunction};
use crate::geo::common::GeoFunctionType;
use crate::json_functions::JsonFunctionType;
use crate::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::scalar::common::ScalarFunctionType;
use crate::scalar::string::TrimType;

use super::cast::CastOperatorType;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ExpressionBuilder {
    // Must be an aggregation function
    pub aggregations: Vec<Expression>,
    pub offset: usize,
    runtime: Arc<Runtime>,
}

struct ParseContext<'a> {
    builder: &'a mut ExpressionBuilder,
    parse_aggregations: bool,
    schema: &'a Schema,
    udfs: &'a [UdfConfig],
}

// Preserves the public crate methods on ExpressionBuilder while delegating
// recursive AST traversal to cohesive free functions operating on ParseContext.
macro_rules! impl_expression_builder_entrypoints {
    () => {
        impl ExpressionBuilder {
            pub fn new(offset: usize, runtime: Arc<Runtime>) -> Self {
                Self {
                    aggregations: Vec::new(),
                    offset,
                    runtime,
                }
            }

            pub fn from(
                offset: usize,
                aggregations: Vec<Expression>,
                runtime: Arc<Runtime>,
            ) -> Self {
                Self {
                    aggregations,
                    offset,
                    runtime,
                }
            }

            pub async fn build(
                &mut self,
                parse_aggregations: bool,
                sql_expression: &SqlExpr,
                schema: &Schema,
                udfs: &[UdfConfig],
            ) -> Result<Expression, Error> {
                self.parse_sql_expression(parse_aggregations, sql_expression, schema, udfs)
                    .await
            }

            pub async fn parse_sql_expression(
                &mut self,
                parse_aggregations: bool,
                expression: &SqlExpr,
                schema: &Schema,
                udfs: &[UdfConfig],
            ) -> Result<Expression, Error> {
                let mut ctx = ParseContext {
                    builder: self,
                    parse_aggregations,
                    schema,
                    udfs,
                };
                parse_expr(expression, &mut ctx).await
            }

            pub fn fullname_from_ident(ident: &[Ident]) -> String {
                ident
                    .iter()
                    .map(|token| token.value.clone())
                    .collect::<Vec<_>>()
                    .join(".")
            }

            pub fn normalize_ident(id: &Ident) -> String {
                id.value.clone()
            }
        }
    };
}

impl_expression_builder_entrypoints!();

#[async_recursion::async_recursion]
async fn parse_expr(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    match expression {
        SqlExpr::Identifier(ident) => parse_sql_column(&[ident.clone()], ctx.schema),
        SqlExpr::CompoundIdentifier(ident) => parse_sql_column(ident, ctx.schema),
        SqlExpr::Value(SqlValue::Number(n, _)) => parse_sql_number(n),
        SqlExpr::Value(SqlValue::Null) => Ok(Expression::Literal(Field::Null)),
        SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
            Ok(Expression::Literal(Field::String(s.to_owned())))
        }
        SqlExpr::UnaryOp { expr, op } => parse_sql_unary_op(op, expr, ctx).await,
        SqlExpr::BinaryOp { .. } => parse_sql_binary_op(expression, ctx).await,
        SqlExpr::Nested(expr) => parse_expr(expr, ctx).await,
        SqlExpr::Function(sql_function) => parse_sql_function(sql_function, ctx).await,
        other => parse_complex_expr(other, ctx).await,
    }
}

async fn parse_complex_expr(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    match expression {
        SqlExpr::Trim { .. } => parse_sql_trim_function(expression, ctx).await,
        SqlExpr::Like { .. } => parse_sql_like_operator(expression, ctx).await,
        SqlExpr::InList { .. } => parse_sql_in_list_operator(expression, ctx).await,
        SqlExpr::Cast { expr, data_type } => parse_sql_cast_operator(expr, data_type, ctx).await,
        SqlExpr::Extract { field, expr } => parse_sql_extract_operator(field, expr, ctx).await,
        SqlExpr::Interval(Interval {
            value,
            leading_field,
            ..
        }) => parse_sql_interval_expression(value, leading_field, ctx).await,
        SqlExpr::Case { .. } => parse_sql_case_expression(expression, ctx).await,
        SqlExpr::IsNull(expr) => parse_sql_isnull_operator(false, expr, ctx).await,
        SqlExpr::IsNotNull(expr) => parse_sql_isnull_operator(true, expr, ctx).await,
        _ => Err(Error::UnsupportedExpression(expression.clone())),
    }
}

fn parse_sql_column(ident: &[Ident], schema: &Schema) -> Result<Expression, Error> {
    let (src_field, src_table_or_alias, src_connection) = split_column_ident(ident)?;
    let matching_by_field: Vec<(usize, &FieldDefinition)> = schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_idx, f)| &f.name == src_field)
        .collect();

    if matching_by_field.len() == 1 {
        return Ok(Expression::Column {
            index: matching_by_field[0].0,
        });
    }
    let Some(table_or_alias) = src_table_or_alias else {
        return Err(Error::InvalidIdent(ident.to_vec()));
    };
    let matching_by_table = filter_candidates(matching_by_field, |source| match source {
        SourceDefinition::Alias { name } | SourceDefinition::Table { name, .. } => {
            name == table_or_alias
        }
        _ => false,
    });
    if matching_by_table.len() == 1 {
        return Ok(Expression::Column {
            index: matching_by_table[0].0,
        });
    }
    let Some(connection) = src_connection else {
        return Err(Error::InvalidIdent(ident.to_vec()));
    };
    let matching_by_conn = filter_candidates(matching_by_table, |source| {
        matches!(source, SourceDefinition::Table { connection: conn, .. } if conn == connection)
    });
    if matching_by_conn.len() == 1 {
        return Ok(Expression::Column {
            index: matching_by_conn[0].0,
        });
    }
    Err(Error::InvalidIdent(ident.to_vec()))
}

async fn parse_sql_trim_function(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let SqlExpr::Trim {
        expr,
        trim_where,
        trim_what,
    } = expression
    else {
        return Err(Error::UnsupportedExpression(expression.clone()));
    };
    let arg = Box::new(parse_expr(expr, ctx).await?);
    let what = match trim_what {
        Some(e) => Some(Box::new(parse_expr(e, ctx).await?)),
        None => None,
    };
    let typ = trim_where.as_ref().map(|e| match e {
        TrimWhereField::Both => TrimType::Both,
        TrimWhereField::Leading => TrimType::Leading,
        TrimWhereField::Trailing => TrimType::Trailing,
    });
    Ok(Expression::Trim { arg, what, typ })
}

async fn parse_function_args(
    args: &[FunctionArg],
    parse_aggregations: bool,
    ctx: &mut ParseContext<'_>,
) -> Result<Vec<Expression>, Error> {
    let prev = ctx.parse_aggregations;
    ctx.parse_aggregations = parse_aggregations;
    let mut parsed = Vec::with_capacity(args.len());
    for arg in args {
        let res = parse_sql_function_arg(arg, ctx).await;
        match res {
            Ok(expr) => parsed.push(expr),
            Err(err) => {
                ctx.parse_aggregations = prev;
                return Err(err);
            }
        }
    }
    ctx.parse_aggregations = prev;
    Ok(parsed)
}

async fn aggr_function_check(
    function_name: &str,
    sql_function: &Function,
    ctx: &mut ParseContext<'_>,
) -> Option<Expression> {
    if !ctx.parse_aggregations {
        return None;
    }
    let aggr = AggregateFunctionType::new(function_name)?;
    let arg_expr = parse_function_args(&sql_function.args, true, ctx)
        .await
        .ok()?;
    let measure = Expression::AggregateFunction {
        fun: aggr,
        args: arg_expr,
    };
    let index = match ctx
        .builder
        .aggregations
        .iter()
        .enumerate()
        .find(|e| e.1 == &measure)
    {
        Some((index, _existing)) => index,
        None => {
            ctx.builder.aggregations.push(measure);
            ctx.builder.aggregations.len() - 1
        }
    };
    Some(Expression::Column {
        index: ctx.builder.offset + index,
    })
}

async fn builtin_function_check(
    function_name: &str,
    sql_function: &Function,
    ctx: &mut ParseContext<'_>,
) -> Option<Expression> {
    if let Some(dtf) = DateTimeFunctionType::new(function_name) {
        return Some(Now { fun: dtf });
    }
    let parse_aggr = ctx.parse_aggregations;
    let args = parse_function_args(&sql_function.args, parse_aggr, ctx)
        .await
        .ok()?;
    if let Some(sft) = ScalarFunctionType::new(function_name) {
        return Some(ScalarFunction { fun: sft, args });
    }
    if let Some(gft) = GeoFunctionType::new(function_name) {
        return Some(GeoFunction { fun: gft, args });
    }
    if let Some(cet) = ConditionalExpressionType::new(function_name) {
        return Some(ConditionalExpression { fun: cet, args });
    }
    if let Some(jft) = JsonFunctionType::new(function_name) {
        return Some(Expression::Json { fun: jft, args });
    }
    None
}

async fn parse_sql_function(
    sql_function: &Function,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let function_name = sql_function.name.to_string().to_lowercase();

    #[cfg(feature = "python")]
    if let Some(udf_name) = function_name.strip_prefix("py_") {
        return parse_python_udf(udf_name, sql_function, ctx).await;
    }

    if let Some(aggr_expr) = aggr_function_check(&function_name, sql_function, ctx).await {
        return Ok(aggr_expr);
    }

    if let Some(builtin_expr) = builtin_function_check(&function_name, sql_function, ctx).await {
        return Ok(builtin_expr);
    }

    if let Some(udf_def) = ctx
        .udfs
        .iter()
        .find(|udf| udf.name == function_name)
        .cloned()
    {
        return parse_configured_udf(&udf_def, sql_function, ctx).await;
    }

    Err(Error::UnknownFunction(function_name))
}

async fn parse_configured_udf(
    udf_def: &UdfConfig,
    sql_function: &Function,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let args = parse_function_args(&sql_function.args, false, ctx).await?;
    match &udf_def.config {
        UdfType::Onnx(config) => {
            #[cfg(feature = "onnx")]
            {
                build_onnx_udf(udf_def.name.clone(), config, args, ctx.schema)
            }
            #[cfg(not(feature = "onnx"))]
            {
                let _ = (config, args);
                Err(Error::OnnxNotEnabled)
            }
        }
        UdfType::JavaScript(config) => {
            #[cfg(feature = "javascript")]
            {
                build_javascript_udf(udf_def.name.clone(), config, args, ctx).await
            }
            #[cfg(not(feature = "javascript"))]
            {
                let _ = (config, args);
                Err(Error::JavaScriptNotEnabled)
            }
        }
        UdfType::Wasm(config) => {
            #[cfg(feature = "wasm")]
            {
                let udf = crate::wasm::Udf::new(
                    udf_def.name.clone(),
                    config.module.clone(),
                    args,
                    ctx.schema,
                )?;
                Ok(Expression::WasmUdf(udf))
            }
            #[cfg(not(feature = "wasm"))]
            {
                let _ = (config, args);
                Err(Error::WasmNotEnabled)
            }
        }
    }
}

async fn parse_sql_function_arg(
    argument: &FunctionArg,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    match argument {
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(arg),
            ..
        }
        | FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => parse_expr(arg, ctx).await,
        FunctionArg::Named {
            arg: FunctionArgExpr::Wildcard,
            ..
        }
        | FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expression::Literal(Field::Null)),
        _ => Err(Error::UnsupportedFunctionArg(argument.clone())),
    }
}

async fn parse_sql_case_expression(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let SqlExpr::Case {
        operand,
        conditions,
        results,
        else_result,
    } = expression
    else {
        return Err(Error::UnsupportedExpression(expression.clone()));
    };
    let op = match operand {
        Some(o) => Some(Box::new(parse_expr(o, ctx).await?)),
        None => None,
    };
    let conds = parse_expr_slice(conditions, ctx).await?;
    let res = parse_expr_slice(results, ctx).await?;
    let else_res = match else_result {
        Some(r) => Some(Box::new(parse_expr(r, ctx).await?)),
        None => None,
    };

    Ok(Expression::Case {
        operand: op,
        conditions: conds,
        results: res,
        else_result: else_res,
    })
}

async fn parse_expr_slice(
    exprs: &[Expr],
    ctx: &mut ParseContext<'_>,
) -> Result<Vec<Expression>, Error> {
    let mut parsed = Vec::with_capacity(exprs.len());
    for expr in exprs {
        parsed.push(parse_expr(expr, ctx).await?);
    }
    Ok(parsed)
}

async fn parse_sql_interval_expression(
    value: &Expr,
    leading_field: &Option<DateTimeField>,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let right = parse_expr(value, ctx).await?;
    if let Some(leading_field) = leading_field {
        Ok(Expression::DateTimeFunction {
            fun: DateTimeFunctionType::Interval {
                field: *leading_field,
            },
            arg: Box::new(right),
        })
    } else {
        Err(Error::MissingLeadingFieldInInterval)
    }
}

async fn parse_sql_unary_op(
    op: &SqlUnaryOperator,
    expr: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let arg = Box::new(parse_expr(expr, ctx).await?);
    let operator = match op {
        SqlUnaryOperator::Not => UnaryOperatorType::Not,
        SqlUnaryOperator::Plus => UnaryOperatorType::Plus,
        SqlUnaryOperator::Minus => UnaryOperatorType::Minus,
        _ => return Err(Error::UnsupportedUnaryOperator(*op)),
    };

    Ok(Expression::UnaryOperator { operator, arg })
}

async fn parse_sql_binary_op(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let SqlExpr::BinaryOp { left, op, right } = expression else {
        return Err(Error::UnsupportedExpression(expression.clone()));
    };
    let left_op = parse_expr(left, ctx).await?;
    let right_op = parse_expr(right, ctx).await?;
    let operator = map_sql_binary_operator(op)?;

    Ok(Expression::BinaryOperator {
        left: Box::new(left_op),
        operator,
        right: Box::new(right_op),
    })
}

#[cfg(not(feature = "bigdecimal"))]
fn parse_sql_number(n: &str) -> Result<Expression, Error> {
    match n.parse::<i64>() {
        Ok(n) => Ok(Expression::Literal(Field::Int(n))),
        Err(_) => match n.parse::<f64>() {
            Ok(f) => Ok(Expression::Literal(Field::Float(OrderedFloat(f)))),
            Err(_) => Err(Error::NotANumber(n.to_string())),
        },
    }
}

#[cfg(feature = "bigdecimal")]
fn parse_sql_number(n: &bigdecimal::BigDecimal) -> Result<Expression, Error> {
    use bigdecimal::ToPrimitive;
    if n.is_integer() {
        Ok(Expression::Literal(Field::Int(n.to_i64().unwrap())))
    } else {
        match n.to_f64() {
            Some(f) => Ok(Expression::Literal(Field::Float(OrderedFloat(f)))),
            None => Err(Error::NotANumber(n.to_string())),
        }
    }
}

async fn parse_sql_like_operator(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let SqlExpr::Like {
        negated,
        expr,
        pattern,
        escape_char,
    } = expression
    else {
        return Err(Error::UnsupportedExpression(expression.clone()));
    };
    let arg = parse_expr(expr, ctx).await?;
    let pattern = parse_expr(pattern, ctx).await?;
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

async fn parse_sql_extract_operator(
    field: &sqlparser::ast::DateTimeField,
    expr: &Expr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let right = parse_expr(expr, ctx).await?;
    Ok(Expression::DateTimeFunction {
        fun: DateTimeFunctionType::Extract { field: *field },
        arg: Box::new(right),
    })
}

async fn parse_sql_cast_operator(
    expr: &Expr,
    data_type: &DataType,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let expression = parse_expr(expr, ctx).await?;
    let cast_to = map_sql_cast_type(data_type)?;
    Ok(Expression::Cast {
        arg: Box::new(expression),
        typ: cast_to,
    })
}

#[cfg(feature = "python")]
async fn parse_python_udf(
    name: &str,
    function: &Function,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    use crate::python_udf::Error::{FailedToParseReturnType, MissingReturnType};

    let args = parse_function_args(&function.args, false, ctx).await?;

    let ident = function
        .return_type
        .as_ref()
        .ok_or_else(|| MissingReturnType)?;
    let return_type = FieldType::try_from(ident.value.as_str()).map_err(FailedToParseReturnType)?;

    Ok(Expression::PythonUDF {
        name: name.to_string(),
        args,
        return_type,
    })
}

#[cfg(feature = "onnx")]
fn build_onnx_udf(
    name: String,
    config: &dozer_types::models::udf_config::OnnxConfig,
    args: Vec<Expression>,
    schema: &Schema,
) -> Result<Expression, Error> {
    use crate::error::Error::Onnx;
    use crate::onnx::error::Error::OnnxOrtErr;
    use crate::onnx::utils::{onnx_input_validation, onnx_output_validation};
    use ort::{Environment, GraphOptimizationLevel, LoggingLevel, SessionBuilder};
    use std::path::Path;

    let environment = Environment::builder()
        .with_name("dozer_onnx")
        .with_log_level(LoggingLevel::Verbose)
        .build()
        .map_err(|e| Onnx(OnnxOrtErr(e)))?
        .into_arc();

    let session = SessionBuilder::new(&environment)
        .map_err(|e| Onnx(OnnxOrtErr(e)))?
        .with_optimization_level(GraphOptimizationLevel::Level1)
        .map_err(|e| Onnx(OnnxOrtErr(e)))?
        .with_intra_threads(1)
        .map_err(|e| Onnx(OnnxOrtErr(e)))?
        .with_model_from_file(Path::new(config.path.as_str()))
        .map_err(|e| Onnx(OnnxOrtErr(e)))?;

    onnx_input_validation(schema, &args, &session.inputs)?;
    onnx_output_validation(&session.outputs)?;

    Ok(Expression::OnnxUDF {
        name,
        session: crate::onnx::DozerSession(session.into()),
        args,
    })
}

#[cfg(feature = "javascript")]
async fn build_javascript_udf(
    name: String,
    config: &dozer_types::models::udf_config::JavaScriptConfig,
    mut args: Vec<Expression>,
    ctx: &ParseContext<'_>,
) -> Result<Expression, Error> {
    use crate::javascript::{validate_args, Udf};
    validate_args(name.clone(), &args, ctx.schema)?;
    let udf = Udf::new(
        ctx.builder.runtime.clone(),
        name,
        config.module.clone(),
        args.remove(0),
    )
    .await?;
    Ok(Expression::JavaScriptUdf(udf))
}

async fn parse_sql_in_list_operator(
    expression: &SqlExpr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let SqlExpr::InList {
        expr,
        list,
        negated,
    } = expression
    else {
        return Err(Error::UnsupportedExpression(expression.clone()));
    };
    let expr = parse_expr(expr, ctx).await?;
    let list_expressions = parse_expr_slice(list, ctx).await?;
    Ok(Expression::InList {
        expr: Box::new(expr),
        list: list_expressions,
        negated: *negated,
    })
}

async fn parse_sql_isnull_operator(
    negated: bool,
    expr: &Expr,
    ctx: &mut ParseContext<'_>,
) -> Result<Expression, Error> {
    let arg = parse_expr(expr, ctx).await?;
    if negated {
        Ok(Expression::IsNotNull { arg: Box::new(arg) })
    } else {
        Ok(Expression::IsNull { arg: Box::new(arg) })
    }
}

fn split_column_ident(
    ident: &[Ident],
) -> Result<(&String, Option<&String>, Option<&String>), Error> {
    match ident.len() {
        1 => Ok((&ident[0].value, None, None)),
        2 => Ok((&ident[1].value, Some(&ident[0].value), None)),
        3 => Ok((
            &ident[2].value,
            Some(&ident[1].value),
            Some(&ident[0].value),
        )),
        _ => Err(Error::InvalidIdent(ident.to_vec())),
    }
}

fn filter_candidates<'a>(
    candidates: Vec<(usize, &'a FieldDefinition)>,
    predicate: impl Fn(&SourceDefinition) -> bool,
) -> Vec<(usize, &'a FieldDefinition)> {
    candidates
        .into_iter()
        .filter(|(_idx, field)| predicate(&field.source))
        .collect()
}

fn map_sql_binary_operator(op: &SqlBinaryOperator) -> Result<BinaryOperatorType, Error> {
    match op {
        SqlBinaryOperator::Gt => Ok(BinaryOperatorType::Gt),
        SqlBinaryOperator::GtEq => Ok(BinaryOperatorType::Gte),
        SqlBinaryOperator::Lt => Ok(BinaryOperatorType::Lt),
        SqlBinaryOperator::LtEq => Ok(BinaryOperatorType::Lte),
        SqlBinaryOperator::Eq => Ok(BinaryOperatorType::Eq),
        SqlBinaryOperator::NotEq => Ok(BinaryOperatorType::Ne),
        SqlBinaryOperator::Plus => Ok(BinaryOperatorType::Add),
        SqlBinaryOperator::Minus => Ok(BinaryOperatorType::Sub),
        SqlBinaryOperator::Multiply => Ok(BinaryOperatorType::Mul),
        SqlBinaryOperator::Divide => Ok(BinaryOperatorType::Div),
        SqlBinaryOperator::Modulo => Ok(BinaryOperatorType::Mod),
        SqlBinaryOperator::And => Ok(BinaryOperatorType::And),
        SqlBinaryOperator::Or => Ok(BinaryOperatorType::Or),
        _ => Err(Error::UnsupportedBinaryOperator(op.clone())),
    }
}

fn map_sql_cast_type(data_type: &DataType) -> Result<CastOperatorType, Error> {
    match data_type {
        DataType::Decimal(_) => Ok(CastOperatorType(FieldType::Decimal)),
        DataType::Binary(_) => Ok(CastOperatorType(FieldType::Binary)),
        DataType::Float(_) => Ok(CastOperatorType(FieldType::Float)),
        DataType::Int(_) | DataType::Integer(_) => Ok(CastOperatorType(FieldType::Int)),
        DataType::UnsignedInt(_) | DataType::UnsignedInteger(_) => {
            Ok(CastOperatorType(FieldType::UInt))
        }
        DataType::Boolean => Ok(CastOperatorType(FieldType::Boolean)),
        DataType::Date => Ok(CastOperatorType(FieldType::Date)),
        DataType::Timestamp(..) => Ok(CastOperatorType(FieldType::Timestamp)),
        DataType::Text => Ok(CastOperatorType(FieldType::Text)),
        DataType::String => Ok(CastOperatorType(FieldType::String)),
        DataType::JSON => Ok(CastOperatorType(FieldType::Json)),
        DataType::Custom(name, ..) => match name.to_string().to_lowercase().as_str() {
            "uint" => Ok(CastOperatorType(FieldType::UInt)),
            "u128" => Ok(CastOperatorType(FieldType::U128)),
            "i128" => Ok(CastOperatorType(FieldType::I128)),
            _ => Err(Error::UnsupportedDataType(data_type.clone())),
        },
        _ => Err(Error::UnsupportedDataType(data_type.clone())),
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
