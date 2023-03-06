use crate::output;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, get_date_field, get_decimal_field, get_ts_field, init_input_schema,
    init_processor, insert_exp, insert_field, update_exp, update_field, DATE8, FIELD_100_FLOAT,
    FIELD_100_INT, FIELD_1_INT, FIELD_200_FLOAT, FIELD_200_INT, FIELD_2_INT, FIELD_3_INT,
    FIELD_50_FLOAT, FIELD_50_INT, FIELD_NULL, ITALY, SINGAPORE,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::FieldType::{Date, Decimal, Float, Int, Timestamp};
use std::collections::HashMap;

#[test]
fn test_count_aggregation_float() {
    let schema = init_input_schema(Float, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_100_FLOAT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2

        Singapore, 50.0
        ---------------
        COUNT = 1
    */
    inp = insert_field(SINGAPORE, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_1_INT),
        update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_3_INT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_3_INT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 2
    */
    inp = delete_field(ITALY, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_int() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2

        Singapore, 50.0
        ---------------
        COUNT = 1
    */
    inp = insert_field(SINGAPORE, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_1_INT),
        update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_3_INT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_3_INT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 2
    */
    inp = delete_field(ITALY, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_decimal() {
    let schema = init_input_schema(Decimal, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, &get_decimal_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        COUNT = 2

        Singapore, 50.0
        ---------------
        COUNT = 1
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(50),
    );
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_1_INT),
        update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_3_INT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 3
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(200),
    );
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_3_INT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        COUNT = 2
    */
    inp = delete_field(ITALY, &get_decimal_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_3_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_int_null() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        COUNT = 2
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_float_null() {
    let schema = init_input_schema(Float, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        COUNT = 2
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        COUNT = 2
    */
    inp = update_field(ITALY, ITALY, &get_decimal_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_timestamp_null() {
    let schema = init_input_schema(Timestamp, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        COUNT = 2
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_count_aggregation_date_null() {
    let schema = init_input_schema(Date, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
            FROM Users \
            WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        COUNT = 2
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_1_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        COUNT = 2
    */
    inp = update_field(ITALY, ITALY, &get_date_field(DATE8), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_2_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        COUNT = 1
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_2_INT, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}
