use crate::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, init_input_schema, init_processor, insert_exp, insert_field,
    update_exp, update_field, FIELD_100_INT, FIELD_150_INT, FIELD_200_INT, FIELD_300_INT,
    FIELD_400_INT, FIELD_500_INT, FIELD_50_INT, FIELD_600_INT, ITALY, SINGAPORE,
};
use crate::output;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::FieldType::Int;
use std::collections::HashMap;

#[test]
fn test_having_insert_delete_ops() {
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
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // 200 -> Insert
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 300 -> Update
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![update_exp(ITALY, ITALY, FIELD_200_INT, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 400 -> Delete
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![delete_exp(ITALY, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 500 -> Nothing
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // Testing Delete

    // 400 -> Nothing
    let inp = delete_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // 300 -> insert
    let inp = delete_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_300_INT)];
    assert_eq!(out, exp);

    // 200 -> update
    let inp = delete_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![update_exp(ITALY, ITALY, FIELD_300_INT, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 100 -> delete
    let inp = delete_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![delete_exp(ITALY, FIELD_200_INT)];
    assert_eq!(out, exp);

    // 0 -> delete
    let inp = delete_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);
}

#[test]
fn test_having_update_ops() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, SUM(Salary) \
            FROM Users \
            HAVING SUM(Salary) > 300 AND SUM(Salary) < 600",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let inp = insert_field(ITALY, FIELD_100_INT);
    let _out = output!(processor, inp);
    let inp = insert_field(ITALY, FIELD_100_INT);
    let _out = output!(processor, inp);

    // 300 -> Nothing
    let inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // 400 -> Insert
    let inp = update_field(ITALY, ITALY, FIELD_200_INT, FIELD_300_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_400_INT)];
    assert_eq!(out, exp);

    // 500 -> Insert
    let inp = update_field(ITALY, ITALY, FIELD_300_INT, FIELD_400_INT);
    let out = output!(processor, inp);
    let exp = vec![update_exp(ITALY, ITALY, FIELD_400_INT, FIELD_500_INT)];
    assert_eq!(out, exp);

    // 600 -> Delete
    let inp = update_field(ITALY, ITALY, FIELD_400_INT, FIELD_500_INT);
    let out = output!(processor, inp);
    let exp = vec![delete_exp(ITALY, FIELD_500_INT)];
    assert_eq!(out, exp);

    // 700 -> Nothing
    let inp = update_field(ITALY, ITALY, FIELD_500_INT, FIELD_600_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // 600 -> Nothing
    let inp = update_field(ITALY, ITALY, FIELD_600_INT, FIELD_500_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);

    // 500 -> insert
    let inp = update_field(ITALY, ITALY, FIELD_500_INT, FIELD_400_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_500_INT)];
    assert_eq!(out, exp);

    // 400 -> update
    let inp = update_field(ITALY, ITALY, FIELD_400_INT, FIELD_300_INT);
    let out = output!(processor, inp);
    let exp = vec![update_exp(ITALY, ITALY, FIELD_500_INT, FIELD_400_INT)];
    assert_eq!(out, exp);

    // 300 -> Delete
    let inp = update_field(ITALY, ITALY, FIELD_300_INT, FIELD_200_INT);
    let out = output!(processor, inp);
    let exp = vec![delete_exp(ITALY, FIELD_400_INT)];
    assert_eq!(out, exp);

    // 200 -> Delete
    let inp = update_field(ITALY, ITALY, FIELD_200_INT, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![];
    assert_eq!(out, exp);
}

#[test]
fn test_having_update_multi_segment_insert_op() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, SUM(Salary) \
            FROM Users GROUP BY Country \
            HAVING SUM(Salary) > 100",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let inp = insert_field(ITALY, FIELD_100_INT);
    let _out = output!(processor, inp);
    let inp = insert_field(SINGAPORE, FIELD_100_INT);
    let _out = output!(processor, inp);

    // 700 -> Nothing
    let inp = update_field(SINGAPORE, ITALY, FIELD_100_INT, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_200_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_having_update_multi_segment_insert_delete_op() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, SUM(Salary) \
            FROM Users GROUP BY Country \
            HAVING SUM(Salary) > 100",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let inp = insert_field(ITALY, FIELD_100_INT);
    let _out = output!(processor, inp);
    let inp = insert_field(SINGAPORE, FIELD_200_INT);
    let _out = output!(processor, inp);

    let inp = update_field(SINGAPORE, ITALY, FIELD_200_INT, FIELD_200_INT);
    let out = output!(processor, inp);
    let exp = vec![
        delete_exp(SINGAPORE, FIELD_200_INT),
        insert_exp(ITALY, FIELD_300_INT),
    ];
    assert_eq!(out, exp);
}

#[test]
fn test_having_update_multi_segment_delete_op() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, SUM(Salary) \
            FROM Users GROUP BY Country \
            HAVING SUM(Salary) > 100",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let inp = insert_field(SINGAPORE, FIELD_50_INT);
    let _out = output!(processor, inp);
    let inp = insert_field(SINGAPORE, FIELD_100_INT);
    let _out = output!(processor, inp);

    let inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    let out = output!(processor, inp);
    let exp = vec![delete_exp(SINGAPORE, FIELD_150_INT)];
    assert_eq!(out, exp);
}
