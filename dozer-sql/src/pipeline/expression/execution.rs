use crate::argv;
use crate::pipeline::errors::PipelineError;

use uuid::Uuid;

use crate::pipeline::expression::geo::common::{get_geo_function_type, GeoFunctionType};
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::{get_scalar_function_type, ScalarFunctionType};
use crate::pipeline::expression::scalar::string::{evaluate_trim, validate_trim, TrimType};
use dozer_types::types::{Field, FieldType, Record, Schema, SourceDefinition};

use super::aggregate::AggregateFunctionType;
use super::cast::CastOperatorType;
use super::scalar::string::{evaluate_like, get_like_operator_type};

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
    #[cfg(feature = "python")]
    PythonUDF {
        name: String,
        args: Vec<Expression>,
        return_type: FieldType,
    },
}

impl Expression {
    pub fn to_string(&self, schema: &Schema) -> String {
        match &self {
            Expression::Column { index } => schema.fields[*index].name.clone(),
            Expression::Literal(value) => value
                .to_string()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
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
            Expression::Cast { arg, typ } => {
                "CAST(".to_string()
                    + arg.to_string(schema).as_str()
                    + " AS "
                    + typ.to_string().as_str()
                    + ")"
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

impl Expression {}

pub trait ExpressionExecutor: Send + Sync {
    fn evaluate(&self, record: &Record, schema: &Schema) -> Result<Field, PipelineError>;
    fn get_type(&self, schema: &Schema) -> Result<ExpressionType, PipelineError>;
}

impl ExpressionExecutor for Expression {
    fn evaluate(&self, record: &Record, schema: &Schema) -> Result<Field, PipelineError> {
        match self {
            Expression::Literal(field) => Ok(field.clone()),
            Expression::Column { index } => Ok(record
                .get_value(*index)
                .map_err(|_e| {
                    PipelineError::InvalidInputType(format!("{} is an invalid field index", *index))
                })?
                .clone()),
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
                use crate::pipeline::expression::python_udf::evaluate_py_udf;
                evaluate_py_udf(schema, name, args, return_type, record)
            }
            Expression::UnaryOperator { operator, arg } => operator.evaluate(schema, arg, record),
            Expression::AggregateFunction { fun, args: _ } => {
                Err(PipelineError::InvalidExpression(format!(
                    "Aggregate Function {fun:?} should not be executed at this point"
                )))
            }
            Expression::Trim { typ, what, arg } => evaluate_trim(schema, arg, what, typ, record),
            Expression::Like {
                arg,
                pattern,
                escape,
            } => evaluate_like(schema, arg, pattern, *escape, record),
            Expression::Cast { arg, typ } => typ.evaluate(schema, arg, record),
            Expression::GeoFunction { fun, args } => fun.evaluate(schema, args, record),
        }
    }

    fn get_type(&self, schema: &Schema) -> Result<ExpressionType, PipelineError> {
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
                    None => Err(PipelineError::InvalidExpression(
                        "literal expression cannot be null".to_string(),
                    )),
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
            Expression::Cast { arg, typ } => typ.get_return_type(schema, arg),
            Expression::GeoFunction { fun, args } => get_geo_function_type(fun, args, schema),
            #[cfg(feature = "python")]
            Expression::PythonUDF { return_type, .. } => Ok(ExpressionType::new(
                *return_type,
                false,
                SourceDefinition::Dynamic,
                false,
            )),
        }
    }
}

fn get_field_type(field: &Field) -> Option<FieldType> {
    match field {
        Field::Int(_) => Some(FieldType::Int),
        Field::Float(_) => Some(FieldType::Float),
        Field::Boolean(_) => Some(FieldType::Boolean),
        Field::String(_) => Some(FieldType::String),
        Field::Binary(_) => Some(FieldType::Binary),
        Field::Decimal(_) => Some(FieldType::Decimal),
        Field::Timestamp(_) => Some(FieldType::Timestamp),
        Field::Bson(_) => Some(FieldType::Bson),
        Field::Null => None,
        Field::UInt(_) => Some(FieldType::UInt),
        Field::Text(_) => Some(FieldType::Text),
        Field::Date(_) => Some(FieldType::Date),
        Field::Point(_) => Some(FieldType::Point),
    }
}

fn get_unary_operator_type(
    operator: &UnaryOperatorType,
    expression: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let field_type = expression.get_type(schema)?;
    match operator {
        UnaryOperatorType::Not => match field_type.return_type {
            FieldType::Boolean => Ok(field_type),
            field_type => Err(PipelineError::InvalidExpression(format!(
                "cannot apply NOT to {field_type:?}"
            ))),
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
) -> Result<ExpressionType, PipelineError> {
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
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {operator:?} to {left_field_type:?} and {right_field_type:?}"
                    )))
                }
            }
        }

        BinaryOperatorType::Add | BinaryOperatorType::Sub | BinaryOperatorType::Mul => {
            match (left_field_type.return_type, right_field_type.return_type) {
                (FieldType::Int, FieldType::Int) => Ok(ExpressionType::new(
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::UInt, FieldType::Int) => Ok(ExpressionType::new(
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Int, FieldType::UInt) => Ok(ExpressionType::new(
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => Ok(ExpressionType::new(
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {operator:?} to {left_field_type:?} and {right_field_type:?}"
                    )))
                }
            }
        }
        BinaryOperatorType::Div | BinaryOperatorType::Mod => {
            match (left_field_type.return_type, right_field_type.return_type) {
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => Ok(ExpressionType::new(
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                    false,
                )),
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {operator:?} to {left_field_type:?} and {right_field_type:?}"
                    )))
                }
            }
        }
    }
}

fn get_aggregate_function_type(
    function: &AggregateFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    match function {
        AggregateFunctionType::Avg => Ok(ExpressionType::new(
            FieldType::Float,
            false,
            SourceDefinition::Dynamic,
            false,
        )),
        AggregateFunctionType::Count => Ok(ExpressionType::new(
            FieldType::Int,
            false,
            SourceDefinition::Dynamic,
            false,
        )),
        AggregateFunctionType::Max => argv!(args, 0, AggregateFunctionType::Max)?.get_type(schema),
        AggregateFunctionType::Median => {
            argv!(args, 0, AggregateFunctionType::Median)?.get_type(schema)
        }
        AggregateFunctionType::Min => argv!(args, 0, AggregateFunctionType::Min)?.get_type(schema),
        AggregateFunctionType::Sum => argv!(args, 0, AggregateFunctionType::Sum)?.get_type(schema),
        AggregateFunctionType::Stddev => Ok(ExpressionType::new(
            FieldType::Float,
            false,
            SourceDefinition::Dynamic,
            false,
        )),
        AggregateFunctionType::Variance => Ok(ExpressionType::new(
            FieldType::Float,
            false,
            SourceDefinition::Dynamic,
            false,
        )),
    }
}
