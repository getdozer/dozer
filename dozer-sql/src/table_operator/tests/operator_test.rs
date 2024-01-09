use std::time::Duration;

use dozer_recordstore::ProcessorRecordStore;
use dozer_sql_expression::execution::Expression;
use dozer_types::{
    chrono::DateTime,
    types::{Field, FieldDefinition, FieldType, Lifetime, Record, Schema, SourceDefinition},
};

use crate::table_operator::{lifetime::LifetimeTableOperator, operator::TableOperator};

#[test]
fn test_lifetime() {
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                "id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Alias {
                    name: "alias".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "ref".to_string(),
                FieldType::Timestamp,
                false,
                SourceDefinition::Alias {
                    name: "alias".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let record_store = ProcessorRecordStore::new(Default::default()).unwrap();
    let record = Record::new(vec![
        Field::Int(0),
        Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
    ]);

    let mut table_operator = LifetimeTableOperator::new(
        None,
        Expression::Column { index: 1 },
        // Expression::new(
        //     ExpressionType::BinaryExpression {
        //         operator: BinaryOperator::Add,
        //         left: Box::new(Expression::new(ExpressionType::Field("ref".to_string()))),
        //         right: Box::new(Expression::new(ExpressionType::Literal(
        //             Literal::Duration(DozerDuration(
        //                 Duration::from_secs(60),
        //                 TimeUnit::Seconds,
        //             )),
        //         ))),
        //     },
        //     "ref".to_string(),
        // ),
        Duration::from_secs(60),
    );

    let result = table_operator
        .execute(&record_store, &record, &schema)
        .unwrap();
    assert_eq!(result.len(), 1);
    let lifetime_record = result.first().unwrap();

    let mut expected_record = record.clone();

    expected_record.set_lifetime(Some(Lifetime {
        reference: DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap(),
        duration: Duration::from_secs(60),
    }));

    assert_eq!(lifetime_record, &expected_record);
}
