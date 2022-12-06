use std::collections::HashMap;

use dozer_core::{
    dag::{executor_local::DEFAULT_PORT_HANDLE, node::Processor},
    storage::transactions::SharedTransaction,
};
use dozer_types::rust_decimal::Decimal;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

use crate::pipeline::aggregation::tests::aggregation_tests_utils::init_processor;

#[test]
fn test_min_aggregation_float() {
    let (mut processor, tx) = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
    )
    .unwrap();

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
            FieldDefinition::new(String::from("Salary"), FieldType::Float, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("MIN(Salary)"), FieldType::Float, false),
            false,
            false,
        )
        .clone();

    let _output_schema = processor
        .update_schema(
            DEFAULT_PORT_HANDLE,
            &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
        )
        .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
                Field::Float(OrderedFloat(100.0)),
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
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
                Field::Float(OrderedFloat(100.0)),
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
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    }];
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
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Float(OrderedFloat(50.0)),
                Field::Float(OrderedFloat(50.0)),
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
                Field::Float(OrderedFloat(50.0)),
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
        MIN = 50.0
    */
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Float(OrderedFloat(50.0)),
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(50.0)),
                Field::Float(OrderedFloat(50.0)),
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
                    Field::Float(OrderedFloat(100.0)),
                ],
            ),
            new: Record::new(
                None,
                vec![
                    Field::String("Italy".to_string()),
                    Field::Float(OrderedFloat(50.0)),
                ],
            ),
        },
        Operation::Delete {
            old: Record::new(
                None,
                vec![
                    Field::String("Singapore".to_string()),
                    Field::Float(OrderedFloat(50.0)),
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
        MIN = 50.0
    */
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(200.0)),
                Field::Float(OrderedFloat(200.0)),
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
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(200.0)),
                Field::Float(OrderedFloat(200.0)),
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
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(50.0)),
                Field::Float(OrderedFloat(50.0)),
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
                Field::Float(OrderedFloat(50.0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = Null
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
                Field::Float(OrderedFloat(100.0)),
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
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_int() {
    let (mut processor, tx) = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
    )
    .unwrap();

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
            FieldDefinition::new(String::from("Salary"), FieldType::Int, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("MIN(Salary)"), FieldType::Int, false),
            false,
            false,
        )
        .clone();

    let _output_schema = processor
        .update_schema(
            DEFAULT_PORT_HANDLE,
            &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
        )
        .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(100),
                Field::Int(100),
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
            vec![Field::String("Italy".to_string()), Field::Int(100)],
        ),
    }];
    assert_eq!(out, exp);

    // Insert another 100 for segment Italy
    /*
        Italy, 100.0
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(100),
                Field::Int(100),
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
            vec![Field::String("Italy".to_string()), Field::Int(100)],
        ),
        new: Record::new(
            None,
            vec![Field::String("Italy".to_string()), Field::Int(100)],
        ),
    }];
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
    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Int(50),
                Field::Int(50),
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
            vec![Field::String("Singapore".to_string()), Field::Int(50)],
        ),
    }];
    assert_eq!(out, exp);

    // Update Singapore segment to Italy
    /*
        Italy, 100.0
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Singapore".to_string()),
                Field::Int(50),
                Field::Int(50),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(50),
                Field::Int(50),
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
                vec![Field::String("Italy".to_string()), Field::Int(100)],
            ),
            new: Record::new(
                None,
                vec![Field::String("Italy".to_string()), Field::Int(50)],
            ),
        },
        Operation::Delete {
            old: Record::new(
                None,
                vec![Field::String("Singapore".to_string()), Field::Int(50)],
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
        MIN = 50.0
    */
    let inp = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(100),
                Field::Int(100),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(200),
                Field::Int(200),
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
            vec![Field::String("Italy".to_string()), Field::Int(50)],
        ),
        new: Record::new(
            None,
            vec![Field::String("Italy".to_string()), Field::Int(50)],
        ),
    }];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(200),
                Field::Int(200),
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
            vec![Field::String("Italy".to_string()), Field::Int(50)],
        ),
        new: Record::new(
            None,
            vec![Field::String("Italy".to_string()), Field::Int(50)],
        ),
    }];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(50),
                Field::Int(50),
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
            vec![Field::String("Italy".to_string()), Field::Int(50)],
        ),
        new: Record::new(
            None,
            vec![Field::String("Italy".to_string()), Field::Int(100)],
        ),
    }];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        MIN = Null
    */
    let inp = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Int(100),
                Field::Int(100),
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
            vec![Field::String("Italy".to_string()), Field::Int(100)],
        ),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_min_aggregation_decimal() {
    let (mut processor, tx) = init_processor(
        "SELECT Country, MIN(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
    )
    .unwrap();

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
            FieldDefinition::new(String::from("MIN(Salary)"), FieldType::Decimal, false),
            false,
            false,
        )
        .clone();

    let _output_schema = processor
        .update_schema(
            DEFAULT_PORT_HANDLE,
            &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
        )
        .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
        MIN = 100.0
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
        MIN = 100.0

        Singapore, 50.0
        -------------
        MIN = 50.0
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
        MIN = 50.0
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
                    Field::Decimal(Decimal::new(50, 0)),
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
        MIN = 50.0
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
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete 1 record (200)
    /*
        Italy, 100.0
        Italy, 50.0
        -------------
        MIN = 50.0
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
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Decimal(Decimal::new(50, 0)),
            ],
        ),
    }];
    assert_eq!(out, exp);

    // Delete another record (50)
    /*
        Italy, 100.0
        -------------
        MIN = 100.0
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
                Field::Decimal(Decimal::new(50, 0)),
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
        MIN = 0.0
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
