use crate::output;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, get_date_field, get_decimal_field, get_ts_field, init_input_schema,
    init_processor, insert_exp, insert_field, update_exp, update_field, DATE16, DATE4, DATE8,
    FIELD_0_FLOAT, FIELD_0_INT, FIELD_100_FLOAT, FIELD_100_INT, FIELD_100_UINT, FIELD_200_FLOAT,
    FIELD_200_INT, FIELD_200_UINT, FIELD_50_FLOAT, FIELD_50_INT, FIELD_50_UINT, FIELD_NULL, ITALY,
    SINGAPORE,
};
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::log::debug;
use dozer_types::types::Field;
use dozer_types::types::FieldType::{Date, Decimal, Float, Int, Text, Timestamp, UInt};
use std::any::Any;
use std::collections::HashMap;

#[test]
fn test_max_aggregator() {
    let max_aggr = Aggregator::Max;
    assert_eq!(Date, max_aggr.get_return_type(Date));
    assert_eq!(Decimal, max_aggr.get_return_type(Decimal));
    assert_eq!(Float, max_aggr.get_return_type(Float));
    assert_eq!(Int, max_aggr.get_return_type(Int));
    assert_eq!(UInt, max_aggr.get_return_type(UInt));
    assert_eq!(Timestamp, max_aggr.get_return_type(Timestamp));
    debug!("{}", max_aggr);
}

#[test]
fn failure_max_aggregator() {
    let schema = init_input_schema(Text, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let mut inp = insert_field(ITALY, &Field::Text("test".to_string()));
    let out = processor
        .aggregate(&mut tx.write(), processor.db.unwrap(), inp)
        .unwrap_err();
    assert_eq!(
        InvalidOperandType("MAX".to_string()).type_id(),
        out.type_id()
    );

    inp = delete_field(ITALY, &Field::Text("test".to_string()));
    let out = processor
        .aggregate(&mut tx.write(), processor.db.unwrap(), inp)
        .unwrap_err();
    assert_eq!(
        InvalidOperandType("MAX".to_string()).type_id(),
        out.type_id()
    );

    inp = update_field(
        ITALY,
        ITALY,
        &Field::Text("test".to_string()),
        &Field::Text("test".to_string()),
    );
    let out = processor
        .aggregate(&mut tx.write(), processor.db.unwrap(), inp)
        .unwrap_err();
    assert_eq!(
        InvalidOperandType("MAX".to_string()).type_id(),
        out.type_id()
    );
}

#[test]
fn test_max_aggregation_float() {
    let schema = init_input_schema(Float, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_FLOAT);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0

        Singapore, 50.0
        ---------------
        MAX = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_FLOAT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_FLOAT),
        update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_100_FLOAT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 200.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_200_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_200_FLOAT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, FIELD_200_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_200_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = Null
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int() {
    let schema = init_input_schema(Int, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0

        Singapore, 50.0
        ---------------
        MAX = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_INT);
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_INT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_INT),
        update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_100_INT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 200.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 50.0
    */
    inp = delete_field(ITALY, FIELD_200_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_200_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = Null
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_100_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_uint() {
    let schema = init_input_schema(UInt, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_UINT);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Insert 50 for segment Singapore
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0

        Singapore, 50.0
        ---------------
        MAX = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_UINT);
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, FIELD_50_UINT)];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_UINT, FIELD_50_UINT);
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_UINT),
        update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_100_UINT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 200.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_UINT, FIELD_200_UINT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_200_UINT)];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 50.0
    */
    inp = delete_field(ITALY, FIELD_200_UINT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_200_UINT, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_UINT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_UINT, FIELD_100_UINT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = Null
    */
    inp = delete_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_100_UINT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal() {
    let schema = init_input_schema(Decimal, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    let mut inp = insert_field(ITALY, &get_decimal_field(100));
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp, tx);
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
        MAX = 100.0

        Singapore, 50.0
        -------------
        MAX = 50.0
    */
    inp = insert_field(SINGAPORE, &get_decimal_field(50));
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, &get_decimal_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(50),
    );
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, &get_decimal_field(50)),
        update_exp(
            ITALY,
            ITALY,
            &get_decimal_field(100),
            &get_decimal_field(100),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 200.0
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(200),
    );
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(200),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, &get_decimal_field(200));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(200),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = delete_field(ITALY, &get_decimal_field(50));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0.0
    */
    inp = delete_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp() {
    let schema = init_input_schema(Timestamp, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100
        -------------
        MAX = 100
    */
    let mut inp = insert_field(ITALY, &get_ts_field(100));
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, &get_ts_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100
        Italy, 100
        -------------
        MAX = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp, tx);
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
        MAX = 100

        Singapore, 50
        -------------
        MAX = 50
    */
    inp = insert_field(SINGAPORE, &get_ts_field(50));
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, &get_ts_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100
        Italy, 100
        Italy, 50
        -------------
        MAX = 100
    */
    inp = update_field(SINGAPORE, ITALY, &get_ts_field(50), &get_ts_field(50));
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, &get_ts_field(50)),
        update_exp(ITALY, ITALY, &get_ts_field(100), &get_ts_field(100)),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200
        Italy, 100
        Italy, 50
        -------------
        MAX = 200
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), &get_ts_field(200));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(100),
        &get_ts_field(200),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100
        Italy, 50
        -------------
        MAX = 100
    */
    inp = delete_field(ITALY, &get_ts_field(200));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(200),
        &get_ts_field(100),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100
        -------------
        MAX = 100
    */
    inp = delete_field(ITALY, &get_ts_field(50));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(100),
        &get_ts_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, &get_ts_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date() {
    let schema = init_input_schema(Date, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, 2015-10-08
        ------------------
        MAX = 2015-10-08
    */
    let mut inp = insert_field(ITALY, &get_date_field(DATE8));
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, &get_date_field(DATE8))];
    assert_eq!(out, exp);

    // Insert another 2015-10-08 for segment Italy
    /*
        Italy, 2015-10-08
        Italy, 2015-10-08
        -----------------
        MAX = 2015-10-08
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp, tx);
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
        MAX = 2015-10-08

        Singapore, 2015-10-04
        -------------
        MAX = 2015-10-04
    */
    inp = insert_field(SINGAPORE, &get_date_field(DATE4));
    out = output!(processor, inp, tx);
    exp = vec![insert_exp(SINGAPORE, &get_date_field(DATE4))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 2015-10-08
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MAX = 2015-10-08
    */
    inp = update_field(
        SINGAPORE,
        ITALY,
        &get_date_field(DATE4),
        &get_date_field(DATE4),
    );
    out = output!(processor, inp, tx);
    exp = vec![
        delete_exp(SINGAPORE, &get_date_field(DATE4)),
        update_exp(ITALY, ITALY, &get_date_field(DATE8), &get_date_field(DATE8)),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 2015-10-16
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MAX = 2015-10-16
    */
    inp = update_field(
        ITALY,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE16),
    );
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE16),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (2015-10-16)
    /*
        Italy, 2015-10-08
        Italy, 2015-10-04
        -------------
        MAX = 2015-10-08
    */
    inp = delete_field(ITALY, &get_date_field(DATE16));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE16),
        &get_date_field(DATE8),
    )];
    assert_eq!(out, exp);

    // Delete another record (2015-10-04)
    /*
        Italy, 2015-10-08
        -------------
        MAX = 2015-10-08
    */
    inp = delete_field(ITALY, &get_date_field(DATE4));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_date_field(DATE8),
        &get_date_field(DATE8),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, &get_date_field(DATE8))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_int_null() {
    let schema = init_input_schema(Int, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, FIELD_0_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX = 100
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_0_INT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_INT, FIELD_0_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_0_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_float_null() {
    let schema = init_input_schema(Float, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX = 0.0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX = 0.0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_FLOAT, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_0_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX = 0.0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100.0
        -------------
        MAX = 100.0
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(0),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX = 0.0
    */
    inp = update_field(ITALY, ITALY, &get_decimal_field(100), FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(100),
        &get_decimal_field(0),
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(0),
        &get_decimal_field(0),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0.0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_timestamp_null() {
    let schema = init_input_schema(Timestamp, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, &get_ts_field(0))];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        MAX = 100
    */
    inp = insert_field(ITALY, &get_ts_field(100));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(0),
        &get_ts_field(100),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = update_field(ITALY, ITALY, &get_ts_field(100), FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_ts_field(100),
        &get_ts_field(0),
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, &get_ts_field(0), &get_ts_field(0))];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, &get_ts_field(0))];
    assert_eq!(out, exp);
}

#[test]
fn test_max_aggregation_date_null() {
    let schema = init_input_schema(Date, "MAX");
    let (processor, tx) = init_processor(
        "SELECT Country, MAX(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    let date_null: &Field = &Field::Date(Utc.timestamp_millis(0).naive_utc().date());

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![insert_exp(ITALY, date_null)];
    assert_eq!(out, exp);

    // Insert 2015-10-08 for segment Italy
    /*
        Italy, NULL
        Italy, 2015-10-08
        -------------
        MAX = 2015-10-08
    */
    inp = insert_field(ITALY, &get_date_field(DATE8));
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, date_null, &get_date_field(DATE8))];
    assert_eq!(out, exp);

    // Update 2015-10-08 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = update_field(ITALY, ITALY, &get_date_field(DATE8), FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, &get_date_field(DATE8), date_null)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, date_null, date_null)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MAX = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, date_null)];
    assert_eq!(out, exp);
}
