use dozer_types::types::{Field, FieldType, Record, Schema};

use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::ScalarFunctionType;

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
    // AggregateFunction {
    //     fun: AggregateFunctionType,
    //     args: Vec<Expression>,
    // },
}

impl Expression {}

pub trait ExpressionExecutor: Send + Sync {
    fn evaluate(&self, record: &Record) -> Field;

    fn get_type(&self, schema: &Schema) -> FieldType;
}

impl ExpressionExecutor for Expression {
    fn evaluate(&self, record: &Record) -> Field {
        match self {
            Expression::Literal(field) => field.clone(),
            Expression::Column { index } => record.values.get(*index).unwrap().clone(),
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => operator.evaluate(left, right, record),
            Expression::ScalarFunction { fun, args } => fun.evaluate(args, record),
            Expression::UnaryOperator { operator, arg } => operator.evaluate(arg, record),
            // Expression::AggregateFunction { fun: _, args: _ } => todo!(),
        }
    }

    fn get_type(&self, schema: &Schema) -> FieldType {
        match self {
            Expression::Literal(field) => get_field_type(field, schema),
            Expression::Column { index } => get_column_type(index, schema),
            Expression::UnaryOperator { operator, arg } => {
                get_unary_operator_type(operator, arg, schema)
            }
            Expression::BinaryOperator {
                left,
                operator,
                right,
            } => get_binary_operator_type(left, operator, right, schema),
            Expression::ScalarFunction { fun, args } => get_scalar_function_type(fun, args, schema),
            // Expression::AggregateFunction { fun, args } => get_aggregate_function_type(fun, args, schema),
        }
    }
}

fn get_field_type(field: &Field, _schema: &Schema) -> FieldType {
    match field {
        Field::Int(_) => FieldType::Int,
        Field::Float(_) => FieldType::Float,
        Field::Boolean(_) => FieldType::Boolean,
        Field::String(_) => FieldType::String,
        Field::Binary(_) => FieldType::Binary,
        Field::Decimal(_) => FieldType::Decimal,
        Field::Timestamp(_) => FieldType::Timestamp,
        Field::Bson(_) => FieldType::Bson,
        Field::RecordArray(_f) => FieldType::Null, //bail!("Record Array not supported: {:?}", f),
        Field::Null => FieldType::Null,
        Field::Invalid(_f) => FieldType::Null, //bail!("Invalid Field Type: {:?}", f)
    }
}

fn get_column_type(index: &usize, schema: &Schema) -> FieldType {
    schema.fields.get(*index).unwrap().typ.clone()
}

fn get_unary_operator_type(
    operator: &UnaryOperatorType,
    expression: &Expression,
    schema: &Schema,
) -> FieldType {
    let field_type = expression.get_type(schema);
    match operator {
        UnaryOperatorType::Not => {
            match field_type {
                FieldType::Boolean => field_type,
                _ => FieldType::Null, //bail!("Invalid Field Type: {:?}", field_type)
            }
        }
        UnaryOperatorType::Plus => field_type,
        UnaryOperatorType::Minus => field_type,
    }
}

fn get_binary_operator_type(
    left: &Expression,
    operator: &BinaryOperatorType,
    right: &Expression,
    schema: &Schema,
) -> FieldType {
    let left_field_type = left.get_type(schema);
    let right_field_type = right.get_type(schema);
    match operator {
        BinaryOperatorType::Eq
        | BinaryOperatorType::Ne
        | BinaryOperatorType::Gt
        | BinaryOperatorType::Gte
        | BinaryOperatorType::Lt
        | BinaryOperatorType::Lte => FieldType::Boolean,

        BinaryOperatorType::And | BinaryOperatorType::Or => {
            match (left_field_type, right_field_type) {
                (FieldType::Boolean, FieldType::Boolean) => FieldType::Boolean,
                _ => FieldType::Null, //bail!("Invalid Field Type: {:?}, {:?}", left_field_type, right_field_type)
            }
        }

        BinaryOperatorType::Add | BinaryOperatorType::Sub | BinaryOperatorType::Mul => {
            match (left_field_type, right_field_type) {
                (FieldType::Int, FieldType::Int) => FieldType::Int,
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => FieldType::Float,
                _ => FieldType::Null, //bail!("Invalid Field Type: {:?}, {:?}", left_field_type, right_field_type)
            }
        }
        BinaryOperatorType::Div | BinaryOperatorType::Mod => {
            match (left_field_type, right_field_type) {
                (FieldType::Int, FieldType::Float)
                | (FieldType::Float, FieldType::Int)
                | (FieldType::Float, FieldType::Float) => FieldType::Float,
                _ => FieldType::Null, //bail!("Invalid Field Type: {:?}, {:?}", left_field_type, right_field_type)
            }
        }
    }
}

// fn get_aggregate_function_type(function: &AggregateFunctionType, args: &[Expression], schema: &Schema) -> FieldType {
//     match function {
//         AggregateFunctionType::Avg => FieldType::Float,
//         AggregateFunctionType::Count => FieldType::Int,
//         AggregateFunctionType::Max => args.get(0).unwrap().get_type(schema),
//         AggregateFunctionType::Median => args.get(0).unwrap().get_type(schema),
//         AggregateFunctionType::Min => args.get(0).unwrap().get_type(schema),
//         AggregateFunctionType::Sum => args.get(0).unwrap().get_type(schema),
//         AggregateFunctionType::Stddev => FieldType::Float,
//         AggregateFunctionType::Variance => FieldType::Float,
//     }
// }

fn get_scalar_function_type(
    function: &ScalarFunctionType,
    args: &[Expression],
    schema: &Schema,
) -> FieldType {
    match function {
        ScalarFunctionType::Abs => args.get(0).unwrap().get_type(schema),
        ScalarFunctionType::Round => FieldType::Int,
    }
}
