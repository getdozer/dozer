use crate::arg_utils::{validate_one_argument, validate_two_arguments};
use crate::case::evaluate_case;
use crate::conditional::{get_conditional_expr_type, ConditionalExpressionType};
use crate::datetime::{get_datetime_function_type, DateTimeFunctionType};
use crate::error::Error;
use crate::geo::common::{get_geo_function_type, GeoFunctionType};
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
}

impl Expression {
    pub fn to_string(&self, schema: &Schema) -> String {
        match &self {
            Expression::Column { index } => schema.fields[*index].name.clone(),
            Expression::Literal(value) => format!("{}", value),
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
                fun.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            Expression::ConditionalExpression { fun, args } => {
                fun.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            Expression::AggregateFunction { fun, args } => {
                fun.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            #[cfg(feature = "python")]
            Expression::PythonUDF { name, args, .. } => {
                name.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|expr| expr.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF { name, args, .. } => {
                name.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|expr| expr.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            Expression::Cast { arg, typ } => {
                "CAST(".to_string()
                    + arg.to_string(schema).as_str()
                    + " AS "
                    + typ.to_string().as_str()
                    + ")"
            }
            Expression::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut op_str = String::new();
                if let Some(op) = operand {
                    op_str += " ";
                    op_str += op.to_string(schema).as_str();
                }
                let mut when_then_str = String::new();
                let iter = zip(conditions, results);
                for (cond, res) in iter {
                    when_then_str += " WHEN ";
                    when_then_str += cond.to_string(schema).as_str();
                    when_then_str += " THEN ";
                    when_then_str += res.to_string(schema).as_str();
                }
                let mut else_str = String::new();
                if let Some(else_res) = else_result {
                    else_str += " ELSE ";
                    else_str += else_res.to_string(schema).as_str();
                }

                "CASE".to_string()
                    + op_str.as_str()
                    + when_then_str.as_str()
                    + else_str.as_str()
                    + " END"
            }
            Expression::Trim { typ, what, arg } => {
                "TRIM(".to_string()
                    + if let Some(t) = typ {
                        t.to_string()
                    } else {
                        "".to_string()
                    }
                    .as_str()
                    + if let Some(w) = what {
                        w.to_string(schema) + " FROM "
                    } else {
                        "".to_string()
                    }
                    .as_str()
                    + arg.to_string(schema).as_str()
                    + ")"
            }
            Expression::Like {
                arg,
                pattern,
                escape: _,
            } => arg.to_string(schema) + " LIKE " + pattern.to_string(schema).as_str(),
            Expression::InList {
                expr,
                list,
                negated,
            } => {
                expr.to_string(schema)
                    + if *negated { " NOT" } else { "" }
                    + " IN ("
                    + list
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            Expression::GeoFunction { fun, args } => {
                fun.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
            Expression::DateTimeFunction { fun, arg } => {
                fun.to_string() + "(" + arg.to_string(schema).as_str() + ")"
            }
            Expression::Now { fun } => fun.to_string() + "()",
            Expression::Json { fun, args } => {
                fun.to_string()
                    + "("
                    + args
                        .iter()
                        .map(|e| e.to_string(schema))
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str()
                    + ")"
            }
        }
    }
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
    pub fn evaluate(&self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        match self {
            Expression::Literal(field) => Ok(field.clone()),
            Expression::Column { index } => Ok(record.values[*index].clone()),
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => operator.evaluate(schema, left, right, record),
            Expression::ScalarFunction { fun, args } => fun.evaluate(schema, args, record),

            #[cfg(feature = "python")]
            Expression::PythonUDF {
                name,
                args,
                return_type,
                ..
            } => {
                use crate::python_udf::evaluate_py_udf;
                evaluate_py_udf(schema, name, args, return_type, record)
            }
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF {
                name: _name,
                session,
                args,
                ..
            } => {
                use std::borrow::Borrow;
                crate::onnx::udf::evaluate_onnx_udf(schema, session.0.borrow(), args, record)
            }

            Expression::UnaryOperator { operator, arg } => operator.evaluate(schema, arg, record),
            Expression::AggregateFunction { fun, args: _ } => {
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
        }
    }

    pub fn get_type(&self, schema: &Schema) -> Result<ExpressionType, Error> {
        match self {
            Expression::Literal(field) => {
                let field_type = get_field_type(field);
                match field_type {
                    Some(f) => Ok(ExpressionType::new(
                        f,
                        false,
                        SourceDefinition::Dynamic,
                        false,
                    )),
                    None => Err(Error::LiteralExpressionIsNull),
                }
            }
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
            Expression::Trim {
                what: _,
                typ: _,
                arg,
            } => validate_trim(arg, schema),
            Expression::Like {
                arg,
                pattern,
                escape: _,
            } => get_like_operator_type(arg, pattern, schema),
            Expression::InList {
                expr: _,
                list: _,
                negated: _,
            } => Ok(ExpressionType::new(
                FieldType::Boolean,
                false,
                SourceDefinition::Dynamic,
                false,
            )),
            Expression::Cast { arg, typ } => typ.get_return_type(schema, arg),
            Expression::GeoFunction { fun, args } => get_geo_function_type(fun, args, schema),
            Expression::DateTimeFunction { fun, arg } => {
                get_datetime_function_type(fun, arg, schema)
            }
            Expression::Now { fun: _ } => Ok(ExpressionType::new(
                FieldType::Timestamp,
                false,
                dozer_types::types::SourceDefinition::Dynamic,
                false,
            )),
            Expression::Json { fun: _, args: _ } => Ok(ExpressionType::new(
                FieldType::Json,
                false,
                dozer_types::types::SourceDefinition::Dynamic,
                false,
            )),
            Expression::Case {
                operand: _,
                conditions: _,
                results,
                else_result: _,
            } => {
                let typ = results.get(0).unwrap().get_type(schema)?;
                Ok(ExpressionType::new(
                    typ.return_type,
                    true,
                    dozer_types::types::SourceDefinition::Dynamic,
                    false,
                ))
            }
            #[cfg(feature = "python")]
            Expression::PythonUDF { return_type, .. } => Ok(ExpressionType::new(
                *return_type,
                false,
                SourceDefinition::Dynamic,
                false,
            )),
            #[cfg(feature = "onnx")]
            Expression::OnnxUDF { .. } => Ok(ExpressionType::new(
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
                false,
            )),
        }
    }
}

pub fn get_field_type(field: &Field) -> Option<FieldType> {
    match field {
        Field::UInt(_) => Some(FieldType::UInt),
        Field::U128(_) => Some(FieldType::U128),
        Field::Int(_) => Some(FieldType::Int),
        Field::I128(_) => Some(FieldType::I128),
        Field::Float(_) => Some(FieldType::Float),
        Field::Boolean(_) => Some(FieldType::Boolean),
        Field::String(_) => Some(FieldType::String),
        Field::Binary(_) => Some(FieldType::Binary),
        Field::Decimal(_) => Some(FieldType::Decimal),
        Field::Timestamp(_) => Some(FieldType::Timestamp),
        Field::Json(_) => Some(FieldType::Json),
        Field::Text(_) => Some(FieldType::Text),
        Field::Date(_) => Some(FieldType::Date),
        Field::Point(_) => Some(FieldType::Point),
        Field::Duration(_) => Some(FieldType::Duration),
        Field::Null => None,
    }
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
            field_type => Err(Error::CannotApplyNotTo(field_type)),
        },
        UnaryOperatorType::Plus => Ok(field_type),
        UnaryOperatorType::Minus => Ok(field_type),
    }
}

fn get_binary_operator_type(
    left: &Expression,
    operator: &BinaryOperatorType,
    right: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    let left_field_type = left.get_type(schema)?;
    let right_field_type = right.get_type(schema)?;
    match operator {
        BinaryOperatorType::Eq
        | BinaryOperatorType::Ne
        | BinaryOperatorType::Gt
        | BinaryOperatorType::Gte
        | BinaryOperatorType::Lt
        | BinaryOperatorType::Lte => Ok(ExpressionType::new(
            FieldType::Boolean,
            false,
            SourceDefinition::Dynamic,
            false,
        )),

        BinaryOperatorType::And | BinaryOperatorType::Or => {
            match (left_field_type.return_type, right_field_type.return_type) {
                (FieldType::Boolean, FieldType::Boolean) => Ok(ExpressionType::new(
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (
                    FieldType::Boolean,
                    FieldType::UInt
                    | FieldType::U128
                    | FieldType::Int
                    | FieldType::I128
                    | FieldType::String
                    | FieldType::Text,
                ) => Ok(ExpressionType::new(
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (
                    FieldType::UInt
                    | FieldType::U128
                    | FieldType::Int
                    | FieldType::I128
                    | FieldType::String
                    | FieldType::Text,
                    FieldType::Boolean,
                ) => Ok(ExpressionType::new(
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (left_field_type, right_field_type) => Err(Error::CannotApplyBinaryOperator {
                    operator: operator.clone(),
                    left_field_type,
                    right_field_type,
                }),
            }
        }

        BinaryOperatorType::Add
        | BinaryOperatorType::Sub
        | BinaryOperatorType::Mul
        | BinaryOperatorType::Mod => {
            match (left_field_type.return_type, right_field_type.return_type) {
                (FieldType::UInt, FieldType::UInt) => Ok(ExpressionType::new(
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::U128, FieldType::U128)
                | (FieldType::U128, FieldType::UInt)
                | (FieldType::UInt, FieldType::U128) => Ok(ExpressionType::new(
                    FieldType::U128,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Timestamp, FieldType::Timestamp) => Ok(ExpressionType::new(
                    FieldType::Duration,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Timestamp, FieldType::Duration) => Ok(ExpressionType::new(
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Duration, FieldType::Timestamp) => Ok(ExpressionType::new(
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Duration, FieldType::Duration) => Ok(ExpressionType::new(
                    FieldType::Duration,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Int, FieldType::Int)
                | (FieldType::Int, FieldType::UInt)
                | (FieldType::UInt, FieldType::Int) => Ok(ExpressionType::new(
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::I128, FieldType::I128)
                | (FieldType::I128, FieldType::UInt)
                | (FieldType::I128, FieldType::U128)
                | (FieldType::I128, FieldType::Int)
                | (FieldType::UInt, FieldType::I128)
                | (FieldType::U128, FieldType::I128)
                | (FieldType::Int, FieldType::I128) => Ok(ExpressionType::new(
                    FieldType::I128,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Float, FieldType::Float)
                | (FieldType::Float, FieldType::UInt)
                | (FieldType::Float, FieldType::U128)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::I128)
                | (FieldType::UInt, FieldType::Float)
                | (FieldType::U128, FieldType::Float)
                | (FieldType::Int, FieldType::Float)
                | (FieldType::I128, FieldType::Float) => Ok(ExpressionType::new(
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Decimal, FieldType::Decimal)
                | (FieldType::UInt, FieldType::Decimal)
                | (FieldType::U128, FieldType::Decimal)
                | (FieldType::Int, FieldType::Decimal)
                | (FieldType::I128, FieldType::Decimal)
                | (FieldType::Float, FieldType::Decimal)
                | (FieldType::Decimal, FieldType::UInt)
                | (FieldType::Decimal, FieldType::U128)
                | (FieldType::Decimal, FieldType::Int)
                | (FieldType::Decimal, FieldType::I128)
                | (FieldType::Decimal, FieldType::Float) => Ok(ExpressionType::new(
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (left_field_type, right_field_type) => Err(Error::CannotApplyBinaryOperator {
                    operator: operator.clone(),
                    left_field_type,
                    right_field_type,
                }),
            }
        }

        BinaryOperatorType::Div => {
            match (left_field_type.return_type, right_field_type.return_type) {
                (FieldType::Int, FieldType::UInt)
                | (FieldType::Int, FieldType::Int)
                | (FieldType::Int, FieldType::U128)
                | (FieldType::Int, FieldType::I128)
                | (FieldType::Int, FieldType::Float)
                | (FieldType::I128, FieldType::UInt)
                | (FieldType::I128, FieldType::Int)
                | (FieldType::I128, FieldType::U128)
                | (FieldType::I128, FieldType::I128)
                | (FieldType::I128, FieldType::Float)
                | (FieldType::UInt, FieldType::UInt)
                | (FieldType::UInt, FieldType::U128)
                | (FieldType::UInt, FieldType::Int)
                | (FieldType::UInt, FieldType::I128)
                | (FieldType::UInt, FieldType::Float)
                | (FieldType::U128, FieldType::UInt)
                | (FieldType::U128, FieldType::U128)
                | (FieldType::U128, FieldType::Int)
                | (FieldType::U128, FieldType::I128)
                | (FieldType::U128, FieldType::Float)
                | (FieldType::Float, FieldType::UInt)
                | (FieldType::Float, FieldType::U128)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::I128)
                | (FieldType::Float, FieldType::Float) => Ok(ExpressionType::new(
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Decimal, FieldType::Decimal)
                | (FieldType::Decimal, FieldType::UInt)
                | (FieldType::Decimal, FieldType::U128)
                | (FieldType::Decimal, FieldType::Int)
                | (FieldType::Decimal, FieldType::I128)
                | (FieldType::Decimal, FieldType::Float)
                | (FieldType::UInt, FieldType::Decimal)
                | (FieldType::U128, FieldType::Decimal)
                | (FieldType::Int, FieldType::Decimal)
                | (FieldType::I128, FieldType::Decimal)
                | (FieldType::Float, FieldType::Decimal) => Ok(ExpressionType::new(
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (left_field_type, right_field_type) => Err(Error::CannotApplyBinaryOperator {
                    operator: operator.clone(),
                    left_field_type,
                    right_field_type,
                }),
            }
        }
    }
}

fn get_aggregate_function_type(
    function: &AggregateFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    match function {
        AggregateFunctionType::Avg => validate_avg(args, schema),
        AggregateFunctionType::Count => validate_count(args, schema),
        AggregateFunctionType::Max => validate_max(args, schema),
        AggregateFunctionType::MaxValue => validate_max_value(args, schema),
        AggregateFunctionType::Min => validate_min(args, schema),
        AggregateFunctionType::MinValue => validate_min_value(args, schema),
        AggregateFunctionType::Sum => validate_sum(args, schema),
    }
}

fn validate_avg(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let arg = validate_one_argument(args, schema, AggregateFunctionType::Avg)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::Decimal,
        FieldType::U128 => FieldType::Decimal,
        FieldType::Int => FieldType::Decimal,
        FieldType::I128 => FieldType::Decimal,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::Avg.to_string(),
                argument_index: 0,
                actual: arg.return_type,
                expected: vec![
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Decimal,
                    FieldType::Duration,
                ],
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

fn validate_count(_args: &[Expression], _schema: &Schema) -> Result<ExpressionType, Error> {
    Ok(ExpressionType::new(
        FieldType::Int,
        false,
        SourceDefinition::Dynamic,
        false,
    ))
}

fn validate_max(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let arg = validate_one_argument(args, schema, AggregateFunctionType::Max)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::Max.to_string(),
                argument_index: 0,
                actual: arg.return_type,
                expected: vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ],
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

fn validate_min(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let arg = validate_one_argument(args, schema, AggregateFunctionType::Min)?;

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::Min.to_string(),
                argument_index: 0,
                actual: arg.return_type,
                expected: vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ],
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

    let ret_type = match arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::Sum.to_string(),
                argument_index: 0,
                actual: arg.return_type,
                expected: vec![
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Decimal,
                    FieldType::Duration,
                ],
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

fn validate_max_value(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let (base_arg, arg) = validate_two_arguments(args, schema, AggregateFunctionType::MaxValue)?;

    match base_arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::MaxValue.to_string(),
                argument_index: 0,
                actual: base_arg.return_type,
                expected: vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ],
            });
        }
    };

    Ok(ExpressionType::new(
        arg.return_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

fn validate_min_value(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let (base_arg, arg) = validate_two_arguments(args, schema, AggregateFunctionType::MinValue)?;

    match base_arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(Error::InvalidFunctionArgumentType {
                function_name: AggregateFunctionType::MinValue.to_string(),
                argument_index: 0,
                actual: base_arg.return_type,
                expected: vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ],
            });
        }
    };

    Ok(ExpressionType::new(
        arg.return_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}
