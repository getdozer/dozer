use std::collections::HashMap;
use std::ops::Div;

use crate::output;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::storage::transactions::SharedTransaction;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};

use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, init_input_schema, init_processor, insert_exp, insert_field,
    update_exp, update_field, FIELD_100_FLOAT, FIELD_100_INT, FIELD_200_FLOAT, FIELD_200_INT,
    FIELD_250_DIV_3_FLOAT, FIELD_250_DIV_3_INT, FIELD_350_DIV_3_FLOAT, FIELD_350_DIV_3_INT,
    FIELD_50_FLOAT, FIELD_50_INT, FIELD_75_FLOAT, FIELD_75_INT, ITALY, SINGAPORE,
};

#[test]
fn test_avg_aggregation_float() {
    let schema = init_input_schema(FieldType::Float, "AVG");
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
        AVG = 100.0
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
        AVG = 100.0
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
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
        AVG = 83.333
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_FLOAT, FIELD_50_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![
        update_exp(ITALY, ITALY, FIELD_100_FLOAT, FIELD_250_DIV_3_FLOAT),
        delete_exp(SINGAPORE, FIELD_50_FLOAT),
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
    out = output!(processor, inp, tx);
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
    out = output!(processor, inp, tx);
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
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_75_FLOAT, FIELD_100_FLOAT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, FIELD_100_FLOAT);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_100_FLOAT)];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_int() {
    let schema = init_input_schema(FieldType::Int, "AVG");
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
        AVG = 100.0
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
        AVG = 100.0
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
        AVG = 100.0

        Singapore, 50.0
        -------------
        AVG = 50.0
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
        AVG = 83.333
    */
    inp = update_field(SINGAPORE, ITALY, FIELD_50_INT, FIELD_50_INT);
    out = output!(processor, inp, tx);
    exp = vec![
        update_exp(ITALY, ITALY, FIELD_100_INT, FIELD_250_DIV_3_INT),
        delete_exp(SINGAPORE, FIELD_50_INT),
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
    out = output!(processor, inp, tx);
    exp = vec![update_exp(
        ITALY,
        ITALY,
        FIELD_250_DIV_3_INT,
        FIELD_350_DIV_3_INT,
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
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_350_DIV_3_INT, FIELD_75_INT)];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    inp = delete_field(ITALY, FIELD_50_INT);
    out = output!(processor, inp, tx);
    exp = vec![update_exp(ITALY, ITALY, FIELD_75_INT, FIELD_100_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp, tx);
    exp = vec![delete_exp(ITALY, FIELD_100_INT)];
    assert_eq!(out, exp);
}

#[test]
fn test_avg_aggregation_decimal() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("ID"), FieldType::Int, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("Country"), FieldType::String, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("Salary"), FieldType::Decimal, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("AVG(Salary)"), FieldType::Decimal, false),
            false,
            false,
        )
        .clone();
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
        AVG = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    };
    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    }];
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
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Singapore".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 83.333
    */
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![
        Operation::Update {
            old: Record::new(
                None,
                vec![
                    Field::String("Italy".to_string()),
                    Field::Decimal(Decimal::new(100, 0)),
                ],
            ),
            new: Record::new(
                None,
                vec![
                    Field::String("Italy".to_string()),
                    Field::Decimal(Decimal::new(250, 0).div(Decimal::new(3, 0))),
                ],
            ),
        },
        Operation::Delete {
            old: Record::new(
                None,
                vec![
                    Field::String("Singapore".to_string()),
                    Field::Decimal(Decimal::new(50, 0)),
                ],
            ),
        },
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
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(200, 0)),
                Field::Decimal(Decimal::new(200, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(250, 0).div(Decimal::new(3, 0))),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(350, 0).div(Decimal::new(3, 0))),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        AVG = 75.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(200, 0)),
                Field::Decimal(Decimal::new(200, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(350, 0).div(Decimal::new(3, 0))),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(75, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        AVG = 100.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(75, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        AVG = 0.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(100, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);
}
