use crate::aggregation::tests::aggregation_tests_utils::{
    delete_field, delete_val_exp, get_date_field, get_decimal_field, get_duration_field,
    get_ts_field, init_input_schema, init_processor, init_val_input_schema, insert_field,
    insert_val_exp, update_field, update_val_exp, DATE16, DATE4, DATE8, FIELD_100_FLOAT,
    FIELD_100_INT, FIELD_100_UINT, FIELD_150_FLOAT, FIELD_150_INT, FIELD_150_UINT, FIELD_200_FLOAT,
    FIELD_200_INT, FIELD_200_UINT, FIELD_NULL, ITALY, SINGAPORE,
};
use crate::output;
use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_types::types::Field;
use dozer_types::types::FieldType::{Date, Decimal, Duration, Float, Int, Timestamp, UInt};
use std::collections::HashMap;

#[test]
fn test_max_aggregation_float() {
    let schema = init_val_input_schema(Float, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, FIELD_100_FLOAT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_150_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_150_FLOAT, FIELD_150_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(SINGAPORE, SINGAPORE, FIELD_100_FLOAT, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(ITALY, SINGAPORE, FIELD_150_FLOAT, FIELD_150_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_150_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int() {
    let schema = init_val_input_schema(Int, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_150_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_150_INT, FIELD_150_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(SINGAPORE, SINGAPORE, FIELD_100_INT, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(ITALY, SINGAPORE, FIELD_150_INT, FIELD_150_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_150_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_uint() {
    let schema = init_val_input_schema(UInt, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, FIELD_100_UINT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_100_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, FIELD_150_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_150_UINT, FIELD_150_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(SINGAPORE, SINGAPORE, FIELD_100_UINT, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(ITALY, SINGAPORE, FIELD_150_UINT, FIELD_150_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, FIELD_150_UINT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal() {
    let schema = init_val_input_schema(Decimal, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, &get_decimal_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_decimal_field(150),
        &get_decimal_field(150),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        SINGAPORE,
        SINGAPORE,
        &get_decimal_field(100),
        &get_decimal_field(200),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_decimal_field(200));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        ITALY,
        SINGAPORE,
        &get_decimal_field(150),
        &get_decimal_field(150),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_decimal_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_duration() {
    let schema = init_val_input_schema(Duration, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, &get_duration_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_duration_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_duration_field(150),
        &get_duration_field(150),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        SINGAPORE,
        SINGAPORE,
        &get_duration_field(100),
        &get_duration_field(200),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_duration_field(200));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        ITALY,
        SINGAPORE,
        &get_duration_field(150),
        &get_duration_field(150),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_duration_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp() {
    let schema = init_val_input_schema(Timestamp, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, &get_ts_field(100));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_ts_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(SINGAPORE, ITALY, &get_ts_field(150), &get_ts_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(SINGAPORE, SINGAPORE, &get_ts_field(100), &get_ts_field(200));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_ts_field(200));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(ITALY, SINGAPORE, &get_ts_field(150), &get_ts_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_ts_field(150));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date() {
    let schema = init_val_input_schema(Date, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -----------------
        MAX_VALUE = Italy
    */
    let mut inp = insert_field(ITALY, &get_date_field(DATE4));
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        -----------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_date_field(DATE4));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Singapore, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = insert_field(SINGAPORE, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Singapore, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE8),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Singapore value 100 -> 200
    /*
        Italy, 100.0
        Singapore, 200.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        SINGAPORE,
        SINGAPORE,
        &get_date_field(DATE4),
        &get_date_field(DATE16),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 150.0
        ---------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_date_field(DATE16));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update Italy segment to Singapore
    /*
        Italy, 100.0
        Singapore, 150.0
        ---------------
        MAX_VALUE = Singapore
    */
    inp = update_field(
        ITALY,
        SINGAPORE,
        &get_date_field(DATE8),
        &get_date_field(DATE8),
    );
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(SINGAPORE.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete another record (150)
    /*
        Italy, 100.0
        -------------
        MAX_VALUE = Italy
    */
    inp = delete_field(SINGAPORE, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(SINGAPORE.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = Null
    */
    inp = delete_field(ITALY, &get_date_field(DATE4));
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int_null() {
    let schema = init_input_schema(Int, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        ----------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX_VALUE = 100
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_float_null() {
    let schema = init_input_schema(Float, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_VALUE = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_VALUE = 100.0
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = update_field(ITALY, ITALY, &Field::String(ITALY.to_string()), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(&Field::String(ITALY.to_string()))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_duration_null() {
    let schema = init_input_schema(Duration, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX_VALUE = 100.0
    */
    inp = insert_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = update_field(ITALY, ITALY, &get_duration_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp_null() {
    let schema = init_input_schema(Timestamp, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX_VALUE = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date_null() {
    let schema = init_input_schema(Date, "MAX_VALUE");
    let mut processor = init_processor(
        "SELECT MAX_VALUE(Salary, Country) FROM Users",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX_VALUE = NULL
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, NULL
        Italy, 2015-10-08
        -------------
        MAX_VALUE = 2015-10-08
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        FIELD_NULL,
        &Field::String(ITALY.to_string()),
    )];
    assert_eq!(out, exp);

    // Update 2015-10-08 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = update_field(ITALY, ITALY, &get_date_field(DATE8), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(
        &Field::String(ITALY.to_string()),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_val_exp(FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX_VALUE = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_val_exp(FIELD_NULL)];
    assert_eq!(out, exp);
}
