use crate::output;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, init_input_schema, init_processor, insert_exp, insert_field,
    update_exp, update_field, FIELD_0_INT, FIELD_100_FLOAT, FIELD_100_INT, FIELD_150_FLOAT,
    FIELD_150_INT, FIELD_200_FLOAT, FIELD_200_INT, FIELD_250_FLOAT, FIELD_250_INT, FIELD_350_FLOAT,
    FIELD_350_INT, FIELD_50_FLOAT, FIELD_50_INT, FIELD_NULL, ITALY, SINGAPORE,
};
use dozer_core::{dag::dag::DEFAULT_PORT_HANDLE, storage::transactions::SharedTransaction};
use dozer_types::types::FieldType::{Float, Int};
use std::collections::HashMap;

#[test]
fn test_sum_aggregation_float() {
    let schema = init_input_schema(Int, "SUM");
    let (processor, tx) = init_processor(
        "SELECT Country, AVG(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        SUM = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);

    let mut exp = vec![insert_exp(ITALY, FIELD_0_INT)];
    assert_eq!(out, exp);
}
