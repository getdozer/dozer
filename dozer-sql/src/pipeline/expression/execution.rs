use crate::argv;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::arg_utils::validate_arg_type;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::{get_scalar_function_type, ScalarFunctionType};
use crate::pipeline::expression::scalar::string::{evaluate_trim, validate_trim};
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema};

use super::aggregate::AggregateFunctionType;

#[derive(Clone, Debug, PartialEq)]
pub enum TrimType {
    Trailing,
    Leading,
    Both,
}

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
    AggregateFunction {
        fun: AggregateFunctionType,
        args: Vec<Expression>,
    },
    Trim {
        arg: Box<Expression>,
        what: Option<Box<Expression>>,
        typ: Option<TrimType>,
    },
}

impl Expression {}

pub trait ExpressionExecutor: Send + Sync {
    fn evaluate(&self, record: &Record, schema: &Schema) -> Result<Field, PipelineError>;
    fn get_type(&self, schema: &Schema) -> Result<FieldType, PipelineError>;
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
            Expression::UnaryOperator { operator, arg } => operator.evaluate(schema, arg, record),
            Expression::AggregateFunction { fun, args: _ } => {
                Err(PipelineError::InvalidExpression(format!(
                    "Aggregate Function {:?} should not be executed at this point",
                    fun
                )))
            }
            Expression::Trim { typ, what, arg } => evaluate_trim(schema, arg, what, typ, record),
        }
    }

    fn get_type(&self, schema: &Schema) -> Result<FieldType, PipelineError> {
        match self {
            Expression::Literal(field) => get_field_type(field).ok_or_else(|| {
                PipelineError::InvalidExpression("literal expression cannot be null".to_string())
            }),
            Expression::Column { index } => Ok(get_column_type(index, schema)),
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
            Expression::Trim { what, typ, arg } => validate_trim(arg, schema),
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
    }
}

fn get_column_type(index: &usize, schema: &Schema) -> FieldType {
    schema.fields.get(*index).unwrap().typ
}

fn get_unary_operator_type(
    operator: &UnaryOperatorType,
    expression: &Expression,
    schema: &Schema,
) -> Result<FieldType, PipelineError> {
    let field_type = expression.get_type(schema)?;
    match operator {
        UnaryOperatorType::Not => match field_type {
            FieldType::Boolean => Ok(field_type),
            field_type => Err(PipelineError::InvalidExpression(format!(
                "cannot apply NOT to {:?}",
                field_type
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
) -> Result<FieldType, PipelineError> {
    let left_field_type = left.get_type(schema)?;
    let right_field_type = right.get_type(schema)?;
    match operator {
        BinaryOperatorType::Eq
        | BinaryOperatorType::Ne
        | BinaryOperatorType::Gt
        | BinaryOperatorType::Gte
        | BinaryOperatorType::Lt
        | BinaryOperatorType::Lte => Ok(FieldType::Boolean),

        BinaryOperatorType::And | BinaryOperatorType::Or => {
            match (left_field_type, right_field_type) {
                (FieldType::Boolean, FieldType::Boolean) => Ok(FieldType::Boolean),
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {:?} to {:?} and {:?}",
                        operator, left_field_type, right_field_type
                    )))
                }
            }
        }

        BinaryOperatorType::Add | BinaryOperatorType::Sub | BinaryOperatorType::Mul => {
            match (left_field_type, right_field_type) {
                (FieldType::Int, FieldType::Int) => Ok(FieldType::Int),
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => Ok(FieldType::Float),
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {:?} to {:?} and {:?}",
                        operator, left_field_type, right_field_type
                    )))
                }
            }
        }
        BinaryOperatorType::Div | BinaryOperatorType::Mod => {
            match (left_field_type, right_field_type) {
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => Ok(FieldType::Float),
                (left_field_type, right_field_type) => {
                    Err(PipelineError::InvalidExpression(format!(
                        "cannot apply {:?} to {:?} and {:?}",
                        operator, left_field_type, right_field_type
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
) -> Result<FieldType, PipelineError> {
    match function {
        AggregateFunctionType::Avg => Ok(FieldType::Float),
        AggregateFunctionType::Count => Ok(FieldType::Int),
        AggregateFunctionType::Max => argv!(args, 0, AggregateFunctionType::Max)?.get_type(schema),
        AggregateFunctionType::Median => {
            argv!(args, 0, AggregateFunctionType::Median)?.get_type(schema)
        }
        AggregateFunctionType::Min => argv!(args, 0, AggregateFunctionType::Min)?.get_type(schema),
        AggregateFunctionType::Sum => argv!(args, 0, AggregateFunctionType::Sum)?.get_type(schema),
        AggregateFunctionType::Stddev => Ok(FieldType::Float),
        AggregateFunctionType::Variance => Ok(FieldType::Float),
    }
}

#[test]
fn test_column_execution() {
    use dozer_types::ordered_float::OrderedFloat;

    let schema = Schema::empty()
        .field(
            FieldDefinition::new("int_field".to_string(), FieldType::Int, false),
            false,
        )
        .field(
            FieldDefinition::new("str_field".to_string(), FieldType::String, false),
            false,
        )
        .field(
            FieldDefinition::new("float_field".to_string(), FieldType::Float, false),
            false,
        )
        .clone();

    let record = Record::new(
        None,
        vec![
            Field::Int(1337),
            Field::String("test".to_string()),
            Field::Float(OrderedFloat(10.10)),
        ],
    );

    // Column
    let e = Expression::Column { index: 0 };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1337)
    );

    let e = Expression::Column { index: 1 };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::String("test".to_string())
    );

    let e = Expression::Column { index: 2 };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(10.10))
    );

    // Literal
    let e = Expression::Literal(Field::Int(1337));
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1337)
    );

    // UnaryOperator
    let e = Expression::UnaryOperator {
        operator: UnaryOperatorType::Not,
        arg: Box::new(Expression::Literal(Field::Boolean(true))),
    };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    );

    // BinaryOperator
    let e = Expression::BinaryOperator {
        left: Box::new(Expression::Literal(Field::Boolean(true))),
        operator: BinaryOperatorType::And,
        right: Box::new(Expression::Literal(Field::Boolean(false))),
    };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false),
    );

    // ScalarFunction
    let e = Expression::ScalarFunction {
        fun: ScalarFunctionType::Abs,
        args: vec![Expression::Literal(Field::Int(-1))],
    };
    assert_eq!(
        e.evaluate(&record, &schema)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1)
    );
}
