use std::time::Duration;

use dozer_types::{
    chrono::DateTime,
    types::{DozerDuration, Field, Record, TimeUnit},
};

use crate::pipeline::table_operator::{lifetime::LifetimeTableOperator, operator::TableOperator};

#[test]
fn test_lifetime() {
    let record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ],
    );

    let table_operator = LifetimeTableOperator::new(
        None,
        DozerDuration(Duration::from_secs(60), TimeUnit::Seconds),
    );

    let result = table_operator.execute(&record).unwrap();
    assert_eq!(result.len(), 1);
    let lifetime_record = result.get(0).unwrap();

    let mut expected_record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ],
    );

    expected_record.set_lifetime(DozerDuration(Duration::from_secs(60), TimeUnit::Seconds));

    assert_eq!(*lifetime_record, expected_record);
}
