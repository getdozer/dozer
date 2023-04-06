use crate::output;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, get_decimal_div_field, get_decimal_field, get_duration_div_field,
    get_duration_field, init_input_schema, init_processor, insert_exp, insert_field, update_exp,
    update_field, FIELD_0_FLOAT, FIELD_100_FLOAT, FIELD_100_INT, FIELD_100_UINT, FIELD_200_FLOAT,
    FIELD_200_INT, FIELD_200_UINT, FIELD_250_DIV_3_FLOAT, FIELD_350_DIV_3_FLOAT, FIELD_50_FLOAT,
    FIELD_50_INT, FIELD_50_UINT, FIELD_75_FLOAT, FIELD_NULL, ITALY, SINGAPORE,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::FieldType::{Decimal, Duration, Float, Int, UInt};
use std::collections::HashMap;

#[test]
fn test_avg_aggregation_float() {
    let schema = init_input_schema(Float, "AVG");
    let mut processor = init_processor(
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
        AVG = 100.0
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
        AVG = 100.0
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
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
        AVG = 83.333
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, FIELD_50_FLOAT),
        update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_250_DIV_3_FLOAT),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 116.667
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        FIELD_250_DIV_3_FLOAT,
        FIELD_350_DIV_3_FLOAT,
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    inp = delete_field(ITALY, FIELD_200_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        FIELD_350_DIV_3_FLOAT,
        FIELD_75_FLOAT,
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_75_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_int() {
    let schema = init_input_schema(Int, "AVG");
    let mut processor = init_processor(
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
        AVG = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_INT);
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_decimal_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 83.333
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_decimal_field(50)),
        update_exp(
            ITALY,
            ITALY,
            &get_decimal_field(100),
            &get_decimal_div_field(250, 3),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 116.667
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_div_field(250, 3),
        &get_decimal_div_field(350, 3),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    inp = delete_field(ITALY, FIELD_200_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_div_field(350, 3),
        &get_decimal_field(75),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(75),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_uint() {
    let schema = init_input_schema(UInt, "AVG");
    let mut processor = init_processor(
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
        AVG = 100.0
    */
    let mut inp = insert_field(ITALY, FIELD_100_UINT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = insert_field(ITALY, FIELD_100_UINT);
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
    */
    inp = insert_field(SINGAPORE, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![insert_exp(SINGAPORE, &get_decimal_field(50))];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 83.333
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_UINT, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![
        delete_exp(SINGAPORE, &get_decimal_field(50)),
        update_exp(
            ITALY,
            ITALY,
            &get_decimal_field(100),
            &get_decimal_div_field(250, 3),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 116.667
    */
    inp = update_field(ITALY, ITALY, FIELD_100_UINT, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_div_field(250, 3),
        &get_decimal_div_field(350, 3),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    inp = delete_field(ITALY, FIELD_200_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_div_field(350, 3),
        &get_decimal_field(75),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_UINT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(75),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, FIELD_100_UINT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_decimal() {
    let schema = init_input_schema(Decimal, "AVG");
    let mut processor = init_processor(
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
        AVG = 100.0
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
        AVG = 100.0
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
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
        AVG = 83.333
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
            &get_decimal_div_field(250, 3),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 116.667
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
        &get_decimal_div_field(250, 3),
        &get_decimal_div_field(350, 3),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    inp = delete_field(ITALY, &get_decimal_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_div_field(350, 3),
        &get_decimal_field(75),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, &get_decimal_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(75),
        &get_decimal_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_duration() {
    let schema = init_input_schema(Duration, "AVG");
    let mut processor = init_processor(
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
        AVG = 100.0
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
        AVG = 100.0
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
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
        AVG = 83.333
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
            &get_duration_div_field(250, 3),
        ),
    ];
    assert_eq!(out, exp);

    // Update Italy value 100 -> 200
    /*
        Italy, 200.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 116.667
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
        &get_duration_div_field(250, 3),
        &get_duration_div_field(350, 3),
    )];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    inp = delete_field(ITALY, &get_duration_field(200));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_div_field(350, 3),
        &get_duration_field(75),
    )];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, &get_duration_field(50));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_duration_field(75),
        &get_duration_field(100),
    )];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, &get_duration_field(100));
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_duration_field(100))];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_int_null() {
    let schema = init_input_schema(Int, "AVG");
    let mut processor = init_processor(
        "SELECT Country, AVG(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        AVG = 100
    */
    inp = insert_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(0),
        &get_decimal_field(50),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_INT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(0),
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
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
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_float_null() {
    let schema = init_input_schema(Float, "AVG");
    let mut processor = init_processor(
        "SELECT Country, AVG(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        AVG = 50
    */
    inp = insert_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_FLOAT, FIELD_50_FLOAT)];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = update_field(ITALY, ITALY, FIELD_100_FLOAT, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_50_FLOAT, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(ITALY, ITALY, FIELD_0_FLOAT, FIELD_0_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_0_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_decimal_null() {
    let schema = init_input_schema(Decimal, "AVG");
    let mut processor = init_processor(
        "SELECT Country, AVG(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert NULL for segment Italy
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    let mut inp = insert_field(ITALY, FIELD_NULL);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, NULL
        Italy, 100
        -------------
        AVG = 50
    */
    inp = insert_field(ITALY, &get_decimal_field(100));
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(0),
        &get_decimal_field(50),
    )];
    assert_eq!(out, exp);

    // Update 100 for segment Italy to NULL
    /*
        Italy, NULL
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = update_field(ITALY, ITALY, &get_decimal_field(100), FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        &get_decimal_field(50),
        &get_decimal_field(0),
    )];
    assert_eq!(out, exp);

    // Delete a record
    /*
        Italy, NULL
        -------------
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
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
        AVG = 0
    */
    inp = delete_field(ITALY, FIELD_NULL);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, &get_decimal_field(0))];
    assert_eq!(out, exp);
}
