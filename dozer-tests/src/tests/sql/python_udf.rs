use crate::tests::sql::{helper, TestInstruction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Field::Float;
use dozer_types::types::Record;

#[test]
fn py_udf_query() {
    let queries = vec![
        r#"
        SELECT py_add(a, 'FLOAT'), py_sum(a, b, 'FLOAT') from t1;
        "#,
        r#"
        SELECT py_binary('BINARY') from t1;
        "#,
        r#"
        SELECT py_null('NULL') from t1;
        "#
    ];

    let record1 = Record {
        schema_id: None,
        values: vec![Float(OrderedFloat(2.0)), Float(OrderedFloat(3.0))],
        version: None,
    };


    let query1_expected_results = Some(vec![record1]);

    let query2_expected_results = Some(vec![]);

    let query3_expected_results = Some(vec![]);

    helper::compare_with_expected_results(
        &[],
        &queries,
        &[query1_expected_results, query2_expected_results, query3_expected_results],
        TestInstruction::List(vec![
            ("t1", "INSERT INTO t1(a, b) VALUES (1, 2)".to_string()),
        ]),
    );
}
