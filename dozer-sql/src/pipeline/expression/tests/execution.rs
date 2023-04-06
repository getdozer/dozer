use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::mathematical::evaluate_sub;
use crate::pipeline::expression::operator::{BinaryOperatorType, UnaryOperatorType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::projection::factory::ProjectionProcessorFactory;
use crate::pipeline::tests::utils::get_select;
use dozer_core::node::ProcessorFactory;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::DateTime;
use dozer_types::types::{
    DozerDuration, Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition, TimeUnit,
};

#[test]
fn test_column_execution() {
    use dozer_types::ordered_float::OrderedFloat;

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "int_field".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "str_field".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "float_field".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
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
        None,
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
            FieldDefinition::new(
                String::from("fn"),
                FieldType::Text,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("ln"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let select = get_select("SELECT count(fn) AS alias1, ln as alias2 FROM t1").unwrap();
    let processor_factory = ProjectionProcessorFactory::_new(select.projection);
    let r = processor_factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(DEFAULT_PORT_HANDLE, (schema, SchemaSQLContext::default()))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .0;

    assert_eq!(
        r,
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("alias1"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("alias2"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic
                ),
                false,
            )
            .clone()
    );
}

#[test]
fn test_wildcard() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("fn"),
                FieldType::Text,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("ln"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let select = get_select("SELECT * FROM t1").unwrap();
    let processor_factory = ProjectionProcessorFactory::_new(select.projection);
    let r = processor_factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(DEFAULT_PORT_HANDLE, (schema, SchemaSQLContext::default()))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .0;

    assert_eq!(
        r,
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ln"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic
                ),
                false,
            )
            .clone()
    );
}

#[test]
fn test_timestamp_difference() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("a"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("b"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let record = Record::new(
        None,
        vec![
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:12:10Z").unwrap()),
        ],
        Some(1),
    );

    let result = evaluate_sub(
        &schema,
        &Expression::Column { index: 0 },
        &Expression::Column { index: 1 },
        &record,
    )
    .unwrap();
    assert_eq!(
        result,
        Field::Duration(DozerDuration(
            std::time::Duration::from_nanos(50000 * 1000 * 1000),
            TimeUnit::Nanoseconds
        ))
    );

    let result = evaluate_sub(
        &schema,
        &Expression::Column { index: 1 },
        &Expression::Column { index: 0 },
        &record,
    );
    assert!(result.is_err());
}
