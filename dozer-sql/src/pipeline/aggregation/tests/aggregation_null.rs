use crate::output;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    init_input_schema, init_processor, FIELD_100_INT,
};
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_types::types::FieldType::{Int};
use dozer_types::types::{Field, Operation, Record};
use std::collections::HashMap;

#[test]
fn test_sum_aggregation_int() {
    let schema = init_input_schema(Int, "SUM");
    let (processor, tx) = init_processor(
        "SELECT Country, SUM(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        NULL, 100.0
        -------------
        SUM = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::Null,
                FIELD_100_INT.clone(),
                FIELD_100_INT.clone(),
            ],
        ),
    };
    let out = output!(processor, inp, tx);
    let exp = vec![Operation::Insert {
        new: Record::new(None, vec![Field::Null, FIELD_100_INT.clone()]),
    }];
    assert_eq!(out, exp);
}
