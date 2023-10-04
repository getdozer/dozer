use crate::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, get_date_field, get_decimal_field, get_duration_field, get_ts_field,
    init_input_schema, init_processor, insert_exp, insert_field, update_exp, update_field, DATE16,
    DATE4, DATE8, FIELD_100_FLOAT, FIELD_100_INT, FIELD_100_UINT, FIELD_200_FLOAT, FIELD_200_INT,
    FIELD_200_UINT, FIELD_50_FLOAT, FIELD_50_INT, FIELD_50_UINT, FIELD_NULL, ITALY, SINGAPORE,
};
use crate::output;
use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_types::types::FieldType::{Date, Decimal, Duration, Float, Int, Timestamp, UInt};
use std::collections::HashMap;

#[test]
fn test_min_aggregation_float() {
    let schema = init_input_schema(Float, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        ---------------
        MIN = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_FLOAT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_FLOAT),
        update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_50_FLOAT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = delete_field(ITALY, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = Null
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_int() {
    let schema = init_input_schema(Int, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        ---------------
        MIN = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_INT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_INT),
        update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_50_INT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_INT, FIELD_50_INT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = delete_field(ITALY, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_INT, FIELD_50_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = Null
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_100_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_uint() {
    let schema = init_input_schema(UInt, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        ---------------
        MIN = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_UINT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_UINT, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_UINT),
        update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_50_UINT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_UINT, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_UINT, FIELD_50_UINT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = delete_field(ITALY, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_UINT, FIELD_50_UINT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_UINT, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = Null
    */
    inp = delete_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_100_UINT)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_decimal() {
    let schema = init_input_schema(Decimal, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        -------------
        MIN = 50.0
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_decimal_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(50),
    );
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_decimal_field(50)),
        update_exp(
            ITALY,
            ITALY,
            &get_decimal_field(100),
            &get_decimal_field(50),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(200),
    );
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(50),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = delete_field(ITALY, &get_decimal_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(50),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    inp = delete_field(ITALY, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_duration() {
    let schema = init_input_schema(Duration, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        -------------
        MIN = 50.0
    */
    inp = insert_field(SINGAPORE, &get_duration_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_duration_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_duration_field(50),
        &get_duration_field(50),
    );
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_duration_field(50)),
        update_exp(
            ITALY,
            ITALY,
            &get_duration_field(100),
            &get_duration_field(50),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_duration_field(100),
        &get_duration_field(200),
    );
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(50),
        &get_duration_field(50),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    inp = delete_field(ITALY, &get_duration_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(50),
        &get_duration_field(50),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    inp = delete_field(ITALY, &get_duration_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(50),
        &get_duration_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_duration_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_timestamp() {
    let schema = init_input_schema(Timestamp, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100
        -------------
        MIN = 100
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
        MIN = 100
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
        MIN = 100

        Singapore, 50
        -------------
        MIN = 50
    */
    inp = insert_field(SINGAPORE, &get_ts_field(50));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_ts_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100
        Italy, 100
        Italy, 50
        -------------
        MIN = 50
    */
    inp = update_field(SINGAPORE, ITALY, &get_ts_field(50), &get_ts_field(50));
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_ts_field(50)),
        update_exp(ITALY, ITALY, &get_ts_field(100), &get_ts_field(50)),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200
        Italy, 100
        Italy, 50
        -------------
        MIN = 50
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), &get_ts_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(50),
        &get_ts_field(50),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100
        Italy, 50
        -------------
        MIN = 50
    */
    inp = delete_field(ITALY, &get_ts_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(50),
        &get_ts_field(50),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100
        -------------
        MIN = 100
    */
    inp = delete_field(ITALY, &get_ts_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(50),
        &get_ts_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_ts_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_date() {
    let schema = init_input_schema(Date, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, 2015-10-08
        ------------------
        MIN = 2015-10-08
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
        MIN = 2015-10-08
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
        MIN = 2015-10-08

        Singapore, 2015-10-04
        -------------
        MIN = 2015-10-04
    */
    inp = insert_field(SINGAPORE, &get_date_field(DATE4));
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_date_field(DATE4))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 2015-10-08
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MIN = 2015-10-04
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_date_field(DATE4),
        &get_date_field(DATE4),
    );
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_date_field(DATE4)),
        update_exp(ITALY, ITALY, &get_date_field(DATE8), &get_date_field(DATE4)),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 2015-10-16
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MIN = 2015-10-04
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE16),
    );
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE4),
        &get_date_field(DATE4),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (2015-10-16)
    /*
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MIN = 2015-10-04
    */
    inp = delete_field(ITALY, &get_date_field(DATE16));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE4),
        &get_date_field(DATE4),
    )];
    assert_eq!(out, exp);

    // Delete another record (2015-10-04)
    /*
        Italy, 2015-10-08
        -------------
        MIN = 2015-10-08
    */
    inp = delete_field(ITALY, &get_date_field(DATE4));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE4),
        &get_date_field(DATE8),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_date_field(DATE8))];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_int_null() {
    let schema = init_input_schema(Int, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = 0
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
        MIN = 0
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = 0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_float_null() {
    let schema = init_input_schema(Float, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = 0.0
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
        MIN = 0.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = NULL
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
        MIN = NULL
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

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = update_field(ITALY, ITALY, &get_decimal_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_duration_null() {
    let schema = init_input_schema(Duration, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = NULL
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
        MIN = NULL
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

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = update_field(ITALY, ITALY, &get_duration_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(100),
        FIELD_NULL,
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_timestamp_null() {
    let schema = init_input_schema(Timestamp, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = 0
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
        MIN = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, &get_ts_field(100))];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = 0
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, &get_ts_field(100), FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_date_null() {
    let schema = init_input_schema(Date, "MIN");
    let mut processor = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MIN = NULL
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
        MIN = 0
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, &get_date_field(DATE8))];
    assert_eq!(out, exp);

    // Update 2015-10-08 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MIN = NULL
    */
    inp = update_field(ITALY, ITALY, &get_date_field(DATE8), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, &get_date_field(DATE8), FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MIN = NULL
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_NULL, FIELD_NULL)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_NULL)];
    assert_eq!(out, exp);
}
