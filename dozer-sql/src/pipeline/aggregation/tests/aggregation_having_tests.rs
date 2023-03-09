use crate::output;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, get_date_field, get_decimal_field, get_ts_field, init_input_schema,
    init_processor, insert_exp, insert_field, update_exp, update_field, DATE8, FIELD_100_FLOAT,
    FIELD_100_INT, FIELD_1_INT, FIELD_200_FLOAT, FIELD_200_INT, FIELD_2_INT, FIELD_300_INT,
    FIELD_3_INT, FIELD_50_FLOAT, FIELD_50_INT, FIELD_NULL, ITALY, SINGAPORE,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::FieldType::{Date, Decimal, Float, Int, Timestamp};
use std::collections::HashMap;

#[test]
fn test_having_all_ops() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, SUM(Salary) \
            FROM Users \
            HAVING SUM(Salary) > 100 AND SUM(Salary) < 400",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Testing insert

    // 100 -> Nothing
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![];
    assert_eq!(out, exp);

    // 200 -> Insert
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 300 -> Update
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![update_exp(ITALY, ITALY, FIELD_200_INT, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 400 -> Delete
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![delete_exp(ITALY, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 500 -> Nothing
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![];
    assert_eq!(out, exp);

    // Testing Delete

    // 400 -> Nothing
    let mut inp = delete_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![];
    assert_eq!(out, exp);

    // 300 -> insert
    let mut inp = delete_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 200 -> update
    let mut inp = delete_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![update_exp(ITALY, ITALY, FIELD_300_INT, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 100 -> delete
    let mut inp = delete_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![delete_exp(ITALY, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 0 -> delete
    let mut inp = delete_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![];
    assert_eq!(out, exp);
}
