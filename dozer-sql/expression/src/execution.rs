use crate::arg_utils::{validate_one_argument, validate_two_arguments};
use crate::case::evaluate_case;
use crate::conditional::{get_conditional_expr_type, ConditionalExpressionType};
use crate::datetime::{get_datetime_function_type, DateTimeFunctionType};
use crate::error::Error;
use crate::geo::common::{get_geo_function_type, GeoFunctionType};
use crate::is_null::{evaluate_is_not_null, evaluate_is_null};
use crate::json_functions::JsonFunctionType;
use crate::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::scalar::common::{get_scalar_function_type, ScalarFunctionType};
use crate::scalar::string::{evaluate_trim, validate_trim, TrimType};
use std::iter::zip;

use super::aggregate::AggregateFunctionType;
use super::cast::CastOperatorType;
use super::in_list::evaluate_in_list;
use super::scalar::string::{evaluate_like, get_like_operator_type};
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Column {
        index: usize,
    },
    Literal(Field),
    UnaryOperator {
        operator: UnaryOperatorType,
        arg: Box<Expression>,
    },
    BinaryOperator {
        left: Box<Expression>,
        operator: BinaryOperatorType,
        right: Box<Expression>,
    },
    ScalarFunction {
        fun: ScalarFunctionType,
        args: Vec<Expression>,
    },
    GeoFunction {
        fun: GeoFunctionType,
        args: Vec<Expression>,
    },
    ConditionalExpression {
        fun: ConditionalExpressionType,
        args: Vec<Expression>,
    },
    DateTimeFunction {
        fun: DateTimeFunctionType,
        arg: Box<Expression>,
    },
    AggregateFunction {
        fun: AggregateFunctionType,
        args: Vec<Expression>,
    },
    Cast {
        arg: Box<Expression>,
        typ: CastOperatorType,
    },
    Trim {
        arg: Box<Expression>,
        what: Option<Box<Expression>>,
        typ: Option<TrimType>,
    },
    Like {
        arg: Box<Expression>,
        pattern: Box<Expression>,
        escape: Option<char>,
    },
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    Now {
        fun: DateTimeFunctionType,
    },
    Json {
        fun: JsonFunctionType,
        args: Vec<Expression>,
    },
    Case {
        operand: Option<Box<Expression>>,
        conditions: Vec<Expression>,
        results: Vec<Expression>,
        else_result: Option<Box<Expression>>,
    },
    IsNull {
        arg: Box<Expression>,
    },
    IsNotNull {
        arg: Box<Expression>,
    },
    #[cfg(feature = "python")]
    PythonUDF {
        name: String,
        args: Vec<Expression>,
        return_type: FieldType,
    },
    #[cfg(feature = "onnx")]
    OnnxUDF {
        name: String,
        session: crate::onnx::DozerSession,
        args: Vec<Expression>,
    },
    #[cfg(feature = "javascript")]
    JavaScriptUdf(crate::javascript::Udf),
    #[cfg(feature = "wasm")]
    WasmUdf(crate::wasm::Udf),
}

impl Expression {
    pub fn to_string(&self, schema: &Schema) -> String {
        match self {
            Expression::Column { index } => schema.fields[*index].name.clone(),
            Expression::Literal(value) => format!("{value}"),
            Expression::UnaryOperator { operator, arg } => {
                operator.to_string() + arg.to_string(schema).as_str()
            }
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => {
                left.to_string(schema)
                    + operator.to_string().as_str()
                    + right.to_string(schema).as_str()
            }
            Expression::ScalarFunction { fun, args } => {
                format_call(&fun.to_string(), args, schema)
            }
            Expression::ConditionalExpression { fun, args } => {
                format_call(&fun.to_string(), args, schema)
            }
            Expression::AggregateFunction { fun, args } => {
                format_call(&fun.to_string(), args, schema)
            }
            Expression::GeoFunction { fun, args } => format_call(&fun.to_string(), args, schema),
            Expression::Json { fun, args } => format_call(&fun.to_string(), args, schema),
            Expression::DateTimeFunction { fun, arg } => {
                format!("{}({})", fun, arg.to_string(schema))
            }
            Expression::Now { fun } => format!("{fun}()"),
            other => other.format_complex_expression(schema),
        }
    }

    fn format_complex_expression(&self, schema: &Schema) -> String {
        match self {
            #[cfg(feature = "python")]
            Expression::PythonUDF { name, args, .. } => format_call(name, args, schema),
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF { name, args, .. } => format_call(name, args, schema),
            #[cfg(feature = "javascript")]
            Expression::JavaScriptUdf(udf) => udf.to_string(schema),
            #[cfg(feature = "wasm")]
            Expression::WasmUdf(udf) => udf.to_string(schema),
            Expression::Cast { arg, typ } => {
                format!("CAST({} AS {})", arg.to_string(schema), typ)
            }
            Expression::Case { .. } => self.format_case_expression(schema),
            Expression::Trim { typ, what, arg } => {
                format_trim_expression(typ.as_ref(), what.as_deref(), arg, schema)
            }
            Expression::Like { arg, pattern, .. } => {
                format!("{} LIKE {}", arg.to_string(schema), pattern.to_string(schema))
            }
            Expression::InList {
                expr,
                list,
                negated,
            } => format_in_list(expr, list, *negated, schema),
            Expression::IsNull { arg } => arg.to_string(schema) + " IS NULL ",
            Expression::IsNotNull { arg } => arg.to_string(schema) + " IS NOT NULL ",
            _ => String::new(),
        }
    }

    fn format_case_expression(&self, schema: &Schema) -> String {
        let Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } = self
        else {
            return String::new();
        };
        let op_str = operand
            .as_deref()
            .map(|op| format!(" {}", op.to_string(schema)))
            .unwrap_or_default();
        let mut when_then_str = String::new();
        for (cond, res) in zip(conditions, results) {
            when_then_str += " WHEN ";
            when_then_str += cond.to_string(schema).as_str();
            when_then_str += " THEN ";
            when_then_str += res.to_string(schema).as_str();
        }
        let else_str = else_result
            .as_deref()
            .map(|else_res| format!(" ELSE {}", else_res.to_string(schema)))
            .unwrap_or_default();
        format!("CASE{op_str}{when_then_str}{else_str} END")
    }
}

fn format_call(name: &str, args: &[Expression], schema: &Schema) -> String {
    let rendered = args
        .iter()
        .map(|e| e.to_string(schema))
        .collect::<Vec<String>>()
        .join(",");
    format!("{name}({rendered})")
}

fn format_in_list(expr: &Expression, list: &[Expression], negated: bool, schema: &Schema) -> String {
    let not_kw = if negated { " NOT" } else { "" };
    let items = list
        .iter()
        .map(|e| e.to_string(schema))
        .collect::<Vec<String>>()
        .join(",");
    format!("{}{not_kw} IN ({items})", expr.to_string(schema))
}

fn format_trim_expression(
    typ: Option<&TrimType>,
    what: Option<&Expression>,
    arg: &Expression,
    schema: &Schema,
) -> String {
    let typ_str = typ.map(ToString::to_string).unwrap_or_default();
    let what_str = what
        .map(|w| w.to_string(schema) + " FROM ")
        .unwrap_or_default();
    format!("TRIM({typ_str}{what_str}{})", arg.to_string(schema))
}

pub struct ExpressionType {
    pub return_type: FieldType,
    pub nullable: bool,
    pub source: SourceDefinition,
    pub is_primary_key: bool,
}

impl ExpressionType {
    pub fn new(
        return_type: FieldType,
        nullable: bool,
        source: SourceDefinition,
        is_primary_key: bool,
    ) -> Self {
        Self {
            return_type,
            nullable,
            source,
            is_primary_key,
        }
    }
}

impl Expression {
    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        match self {
            Expression::Literal(field) => Ok(field.clone()),
            Expression::Column { index } => Ok(record.values[*index].clone()),
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => operator.evaluate(schema, left, right, record),
            Expression::ScalarFunction { fun, args } => fun.evaluate(schema, args, record),
            Expression::UnaryOperator { operator, arg } => operator.evaluate(schema, arg, record),
            Expression::AggregateFunction { fun, .. } => {
                Err(Error::UnexpectedAggregationExecution(fun.clone()))
            }
            Expression::Trim { typ, what, arg } => evaluate_trim(schema, arg, what, typ, record),
            Expression::Like {
                arg,
                pattern,
                escape,
            } => evaluate_like(schema, arg, pattern, *escape, record),
            Expression::InList {
                expr,
                list,
                negated,
            } => evaluate_in_list(schema, expr, list, *negated, record),
            Expression::Cast { arg, typ } => typ.evaluate(schema, arg, record),
            Expression::GeoFunction { fun, args } => fun.evaluate(schema, args, record),
            Expression::ConditionalExpression { fun, args } => fun.evaluate(schema, args, record),
            Expression::DateTimeFunction { fun, arg } => fun.evaluate(schema, arg, record),
            Expression::Now { fun } => fun.evaluate_now(),
            Expression::Json { fun, args } => fun.evaluate(schema, args, record),
            Expression::Case {
                operand,
                conditions,
                results,
                else_result,
            } => evaluate_case(schema, operand, conditions, results, else_result, record),
            Expression::IsNull { arg } => evaluate_is_null(schema, arg, record),
            Expression::IsNotNull { arg } => evaluate_is_not_null(schema, arg, record),
            other => other.evaluate_udf(record, schema),
        }
    }

    fn evaluate_udf(&mut self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        match self {
            #[cfg(feature = "python")]
            Expression::PythonUDF {
                name,
                args,
                return_type,
            } => crate::python_udf::evaluate_py_udf(schema, name, args, return_type, record),
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF { session, args, .. } => {
                use std::borrow::Borrow;
                crate::onnx::udf::evaluate_onnx_udf(schema, session.0.borrow(), args, record)
            }
            #[cfg(feature = "javascript")]
            Expression::JavaScriptUdf(udf) => udf.evaluate(record, schema),
            #[cfg(feature = "wasm")]
            Expression::WasmUdf(udf) => udf.evaluate(record, schema),
            _ => unreachable!("non-UDF expression routed to evaluate_udf"),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> Result<ExpressionType, Error> {
        match self {
            Expression::Literal(field) => field
                .ty()
                .map(|f| ExpressionType::new(f, false, SourceDefinition::Dynamic, false))
                .ok_or(Error::LiteralExpressionIsNull),
            Expression::Column { index } => {
                let t = schema.fields.get(*index).unwrap();
                Ok(ExpressionType::new(
                    t.typ,
                    t.nullable,
                    t.source.clone(),
                    schema.primary_index.contains(index),
                ))
            }
            Expression::UnaryOperator { operator, arg } => {
                get_unary_operator_type(operator, arg, schema)
            }
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => get_binary_operator_type(left, operator, right, schema),
            Expression::ScalarFunction { fun, args } => get_scalar_function_type(fun, args, schema),
            Expression::ConditionalExpression { fun, args } => {
                get_conditional_expr_type(fun, args, schema)
            }
            Expression::AggregateFunction { fun, args } => {
                get_aggregate_function_type(fun, args, schema)
            }
            Expression::Trim { arg, .. } => validate_trim(arg, schema),
            Expression::Like { arg, pattern, .. } => get_like_operator_type(arg, pattern, schema),
            Expression::Cast { arg, typ } => typ.get_return_type(schema, arg),
            Expression::GeoFunction { fun, args } => get_geo_function_type(fun, args, schema),
            Expression::DateTimeFunction { fun, arg } => {
                get_datetime_function_type(fun, arg, schema)
            }
            other => other.get_special_or_udf_type(schema),
        }
    }

    fn get_special_or_udf_type(&self, schema: &Schema) -> Result<ExpressionType, Error> {
        match self {
            Expression::InList { .. }
            | Expression::IsNull { .. }
            | Expression::IsNotNull { .. } => Ok(dynamic_non_null_type(FieldType::Boolean)),
            Expression::Now { .. } => Ok(dynamic_non_null_type(FieldType::Timestamp)),
            Expression::Json { .. } => Ok(dynamic_non_null_type(FieldType::Json)),
            Expression::Case { results, .. } => {
                let typ = results.first().unwrap().get_type(schema)?;
                Ok(ExpressionType::new(
                    typ.return_type,
                    true,
                    SourceDefinition::Dynamic,
                    false,
                ))
            }
            #[cfg(feature = "python")]
            Expression::PythonUDF { return_type, .. } => Ok(dynamic_non_null_type(*return_type)),
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF { .. } => Ok(dynamic_non_null_type(FieldType::Float)),
            #[cfg(feature = "javascript")]
            Expression::JavaScriptUdf(udf) => Ok(udf.get_type()),
            #[cfg(feature = "wasm")]
            Expression::WasmUdf(udf) => Ok(udf.get_type()),
            _ => unreachable!("handled in get_type"),
        }
    }
}

fn dynamic_non_null_type(field_type: FieldType) -> ExpressionType {
    ExpressionType::new(field_type, false, SourceDefinition::Dynamic, false)
}

fn get_unary_operator_type(
    operator: &UnaryOperatorType,
    expression: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    let field_type = expression.get_type(schema)?;
    match operator {
        UnaryOperatorType::Not => match field_type.return_type {
            FieldType::Boolean => Ok(field_type),
            other => Err(Error::CannotApplyNotTo(other)),
        },
        UnaryOperatorType::Plus | UnaryOperatorType::Minus => Ok(field_type),
    }
}

fn get_binary_operator_type(
    left: &Expression,
    operator: &BinaryOperatorType,
    right: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    let left_ty = left.get_type(schema)?.return_type;
    let right_ty = right.get_type(schema)?.return_type;
    match operator {
        BinaryOperatorType::Eq
        | BinaryOperatorType::Ne
        | BinaryOperatorType::Gt
        | BinaryOperatorType::Gte
        | BinaryOperatorType::Lt
        | BinaryOperatorType::Lte => Ok(dynamic_non_null_type(FieldType::Boolean)),
        BinaryOperatorType::And | BinaryOperatorType::Or => {
            resolve_logical_binary_type(operator, left_ty, right_ty)
        }
        BinaryOperatorType::Add
        | BinaryOperatorType::Sub
        | BinaryOperatorType::Mul
        | BinaryOperatorType::Mod => resolve_arithmetic_binary_type(operator, left_ty, right_ty),
        BinaryOperatorType::Div => resolve_division_binary_type(operator, left_ty, right_ty),
    }
}

const LOGICAL_COERCIBLE_TYPES: [FieldType; 7] = [
    FieldType::Boolean,
    FieldType::UInt,
    FieldType::U128,
    FieldType::Int,
    FieldType::I128,
    FieldType::String,
    FieldType::Text,
];

const NUMERIC_RANK_ORDER: [(FieldType, u8); 6] = [
    (FieldType::UInt, 1),
    (FieldType::U128, 2),
    (FieldType::Int, 3),
    (FieldType::I128, 4),
    (FieldType::Float, 5),
    (FieldType::Decimal, 6),
];

const SUMMABLE_EXPECTED: [FieldType; 7] = [
    FieldType::UInt,
    FieldType::U128,
    FieldType::Int,
    FieldType::I128,
    FieldType::Float,
    FieldType::Decimal,
    FieldType::Duration,
];

const COMPARABLE_EXPECTED: [FieldType; 9] = [
    FieldType::Decimal,
    FieldType::UInt,
    FieldType::U128,
    FieldType::Int,
    FieldType::I128,
    FieldType::Float,
    FieldType::Timestamp,
    FieldType::Date,
    FieldType::Duration,
];

fn is_valid_logical_pair(left_ty: FieldType, right_ty: FieldType) -> bool {
    if left_ty == FieldType::Boolean {
        return LOGICAL_COERCIBLE_TYPES.contains(&right_ty);
    }
    if right_ty == FieldType::Boolean {
        return LOGICAL_COERCIBLE_TYPES.contains(&left_ty);
    }
    false
}

fn resolve_logical_binary_type(
    operator: &BinaryOperatorType,
    left_ty: FieldType,
    right_ty: FieldType,
) -> Result<ExpressionType, Error> {
    if is_valid_logical_pair(left_ty, right_ty) {
        return Ok(dynamic_non_null_type(FieldType::Boolean));
    }
    Err(Error::CannotApplyBinaryOperator {
        operator: operator.clone(),
        left_field_type: left_ty,
        right_field_type: right_ty,
    })
}

fn numeric_rank(ty: FieldType) -> Option<u8> {
    NUMERIC_RANK_ORDER
        .into_iter()
        .find_map(|(candidate, rank)| (candidate == ty).then_some(rank))
}

fn resolve_arithmetic_binary_type(
    operator: &BinaryOperatorType,
    left_ty: FieldType,
    right_ty: FieldType,
) -> Result<ExpressionType, Error> {
    if let Some(ts_type) = resolve_temporal_arithmetic_type(left_ty, right_ty) {
        return Ok(dynamic_non_null_type(ts_type));
    }
    if let (Some(l_rank), Some(r_rank)) = (numeric_rank(left_ty), numeric_rank(right_ty)) {
        let promoted = if l_rank >= r_rank { left_ty } else { right_ty };
        return Ok(dynamic_non_null_type(promoted));
    }
    Err(Error::CannotApplyBinaryOperator {
        operator: operator.clone(),
        left_field_type: left_ty,
        right_field_type: right_ty,
    })
}

fn resolve_temporal_arithmetic_type(left_ty: FieldType, right_ty: FieldType) -> Option<FieldType> {
    match (left_ty, right_ty) {
        (FieldType::Timestamp, FieldType::Timestamp)
        | (FieldType::Duration, FieldType::Duration) => Some(FieldType::Duration),
        (FieldType::Timestamp, FieldType::Duration)
        | (FieldType::Duration, FieldType::Timestamp) => Some(FieldType::Timestamp),
        _ => None,
    }
}

fn resolve_division_binary_type(
    operator: &BinaryOperatorType,
    left_ty: FieldType,
    right_ty: FieldType,
) -> Result<ExpressionType, Error> {
    if numeric_rank(left_ty).is_none() || numeric_rank(right_ty).is_none() {
        return Err(Error::CannotApplyBinaryOperator {
            operator: operator.clone(),
            left_field_type: left_ty,
            right_field_type: right_ty,
        });
    }
    if left_ty == FieldType::Decimal || right_ty == FieldType::Decimal {
        Ok(dynamic_non_null_type(FieldType::Decimal))
    } else {
        Ok(dynamic_non_null_type(FieldType::Float))
    }
}

fn get_aggregate_function_type(
    function: &AggregateFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    match function {
        AggregateFunctionType::Avg => validate_avg(args, schema),
        AggregateFunctionType::Count => Ok(dynamic_non_null_type(FieldType::Int)),
        AggregateFunctionType::Max
        | AggregateFunctionType::MaxAppendOnly
        | AggregateFunctionType::Min
        | AggregateFunctionType::MinAppendOnly => {
            let arg = validate_one_argument(args, schema, function.clone())?;
            ensure_comparable_arg(arg.return_type, arg.return_type, function)
        }
        AggregateFunctionType::MaxValue | AggregateFunctionType::MinValue => {
            let (base_arg, arg) = validate_two_arguments(args, schema, function.clone())?;
            ensure_comparable_arg(base_arg.return_type, arg.return_type, function)
        }
        AggregateFunctionType::Sum => validate_sum(args, schema),
    }
}

fn validate_avg(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let arg = validate_one_argument(args, schema, AggregateFunctionType::Avg)?;
    let ret_type = match arg.return_type {
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::Int8
        | FieldType::I128
        | FieldType::Decimal => FieldType::Decimal,
        FieldType::Float => FieldType::Float,
        FieldType::Duration => FieldType::Duration,
        actual => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::Avg.to_string(),
                argument_index: 0,
                actual,
                expected: SUMMABLE_EXPECTED.to_vec(),
            });
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

fn validate_sum(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let arg = validate_one_argument(args, schema, AggregateFunctionType::Sum)?;
    if arg.return_type == FieldType::Int8 || SUMMABLE_EXPECTED.contains(&arg.return_type) {
        return Ok(ExpressionType::new(
            arg.return_type,
            true,
            SourceDefinition::Dynamic,
            false,
        ));
    }
    Err(Error::InvalidFunctionArgumentType {
        function_name: AggregateFunctionType::Sum.to_string(),
        argument_index: 0,
        actual: arg.return_type,
        expected: SUMMABLE_EXPECTED.to_vec(),
    })
}

fn ensure_comparable_arg(
    check_ty: FieldType,
    return_ty: FieldType,
    func: &AggregateFunctionType,
) -> Result<ExpressionType, Error> {
    if check_ty == FieldType::Int8 || COMPARABLE_EXPECTED.contains(&check_ty) {
        return Ok(ExpressionType::new(
            return_ty,
            true,
            SourceDefinition::Dynamic,
            false,
        ));
    }
    Err(Error::InvalidFunctionArgumentType {
        function_name: func.to_string(),
        argument_index: 0,
        actual: check_ty,
        expected: COMPARABLE_EXPECTED.to_vec(),
    })
}
