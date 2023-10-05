use crate::aggregation::tests::aggregation_tests_utils::{
    get_date_field, get_decimal_field, get_duration_field, get_ts_field, init_input_schema,
    init_processor, insert_exp, insert_field, update_exp, DATE4, DATE8, FIELD_100_FLOAT,
    FIELD_100_INT, FIELD_100_UINT, FIELD_50_FLOAT, FIELD_50_INT, FIELD_50_UINT, FIELD_NULL, ITALY,
    SINGAPORE,
};
use crate::output;
use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_types::types::FieldType::{Date, Decimal, Duration, Float, Int, Timestamp, UInt};
use std::collections::HashMap;

#[test]
fn test_max_aggregation_float() {
    let schema = init_input_schema(Float, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_FLOAT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0

        Singapore, 50.0
        ---------------
        MAX_APPEND_ONLY = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int() {
    let schema = init_input_schema(Int, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0

        Singapore, 50.0
        ---------------
        MAX_APPEND_ONLY = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_uint() {
    let schema = init_input_schema(UInt, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_UINT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0

        Singapore, 50.0
        ---------------
        MAX_APPEND_ONLY = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_UINT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal() {
    let schema = init_input_schema(Decimal, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    let mut inp = insert_field(ITALY, &get_decimal_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0

        Singapore, 50.0
        -------------
        MAX_APPEND_ONLY = 50.0
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_decimal_field(50))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_duration() {
    let schema = init_input_schema(Duration, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    let mut inp = insert_field(ITALY, &get_duration_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_duration_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(100),
        &get_duration_field(100),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0

        Singapore, 50.0
        -------------
        MAX_APPEND_ONLY = 50.0
    */
    inp = insert_field(SINGAPORE, &get_duration_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_duration_field(50))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp() {
    let schema = init_input_schema(Timestamp, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100
        -------------
        MAX_APPEND_ONLY = 100
    */
    let mut inp = insert_field(ITALY, &get_ts_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_ts_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100
        Italy, 100
        -------------
        MAX_APPEND_ONLY = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(100),
        &get_ts_field(100),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100
        Italy, 100
        -------------
        MAX_APPEND_ONLY = 100

        Singapore, 50
        -------------
        MAX_APPEND_ONLY = 50
    */
    inp = insert_field(SINGAPORE, &get_ts_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_ts_field(50))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date() {
    let schema = init_input_schema(Date, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, 2015-10-08
        ------------------
        MAX_APPEND_ONLY = 2015-10-08
    */
    let mut inp = insert_field(ITALY, &get_date_field(DATE8));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_date_field(DATE8))];
    assert_eq!(out, exp);

    // Insert another 2015-10-08 for segment Italy
    /*
        Italy, 2015-10-08
        Italy, 2015-10-08
        -----------------
        MAX_APPEND_ONLY = 2015-10-08
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE8),
    )];
    assert_eq!(out, exp);

    // Insert 2015-10-04 for segment Singapore
    /*
        Italy, 2015-10-08
        Italy, 2015-10-08
        -------------
        MAX_APPEND_ONLY = 2015-10-08

        Singapore, 2015-10-04
        -------------
        MAX_APPEND_ONLY = 2015-10-04
    */
    inp = insert_field(SINGAPORE, &get_date_field(DATE4));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_date_field(DATE4))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int_null() {
    let schema = init_input_schema(Int, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX_APPEND_ONLY = 100
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_100_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_float_null() {
    let schema = init_input_schema(Float, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_100_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        FIELD_NULL,
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_duration_null() {
    let schema = init_input_schema(Duration, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_APPEND_ONLY = 100.0
    */
    inp = insert_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        FIELD_NULL,
        &get_duration_field(100),
    )];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp_null() {
    let schema = init_input_schema(Timestamp, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX_APPEND_ONLY = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, &get_ts_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date_null() {
    let schema = init_input_schema(Date, "MAX_APPEND_ONLY");
    let mut processor = init_processor(
        "SELECT Country, MAX_APPEND_ONLY(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_APPEND_ONLY = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, NULL
        Italy, 2015-10-08
        -------------
        MAX_APPEND_ONLY = 2015-10-08
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, &get_date_field(DATE8))];
    assert_eq!(out, exp);
}
