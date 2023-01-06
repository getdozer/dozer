use crate::pipeline::builder::get_select;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::projection::factory::ProjectionProcessorFactory;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::ProcessorFactory;
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

#[test]
fn test_alias() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("fn"), FieldType::Text, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("ln"), FieldType::String, false),
            false,
        )
        .clone();

    let select = get_select("SELECT count(fn) AS alias1, ln as alias2 FROM t1").unwrap();
    let processor_factory = ProjectionProcessorFactory::_new(select.projection);
    let r = processor_factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(DEFAULT_PORT_HANDLE, schema.clone())]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(
        r,
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("alias1"), FieldType::Text, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("alias2"), FieldType::String, false),
                false,
            )
            .clone()
    );
}
