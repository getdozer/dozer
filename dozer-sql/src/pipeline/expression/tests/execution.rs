use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema};

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
