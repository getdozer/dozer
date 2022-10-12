use crate::aggregation::groupby::{AggregationProcessor, FieldRule};
use crate::aggregation::sum::IntegerSumAggregator;
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;
use crate::state::StateStoresManager;
use dozer_types::types::{Field, Operation, Record};
use std::sync::Arc;
use std::{env, fs};
use tempdir::TempDir;

#[test]
fn test_insert_update_delete() {
    let mut store = MemoryStateStore::new();

    let rules = vec![
        FieldRule::Dimension(0, true, Some("City".to_string())),
        FieldRule::Dimension(1, true, Some("Country".to_string())),
        FieldRule::Measure(
            3,
            Box::new(IntegerSumAggregator::new()),
            true,
            Some("Sum".to_string()),
        ),
    ];
    let agg = AggregationProcessor::new(rules);

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        Operation::Insert {
            new: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ]
            )
        }
    );

    // Insert another 10
    let i = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        Operation::Update {
            old: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ]
            ),
            new: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(20)
                ]
            )
        }
    );

    // update from 10 to 5
    let i = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(5),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        Operation::Update {
            old: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(20)
                ]
            ),
            new: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(15)
                ]
            )
        }
    );

    // Delete 5
    let i = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(5),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        Operation::Update {
            old: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(15)
                ]
            ),
            new: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ]
            )
        }
    );

    // Delete last 10
    let i = Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        Operation::Delete {
            old: Record::new(
                None,
                vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ]
            )
        }
    );
}

#[test]
fn test_insert_update_change_dims() {
    let mut store = MemoryStateStore::new();

    let rules = vec![
        FieldRule::Dimension(0, true, Some("City".to_string())),
        FieldRule::Dimension(1, true, Some("Country".to_string())),
        FieldRule::Measure(
            3,
            Box::new(IntegerSumAggregator::new()),
            true,
            Some("Sum".to_string()),
        ),
    ];

    let agg = AggregationProcessor::new(rules);

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    assert!(agg.aggregate(&mut store, i).is_ok());

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    assert!(agg.aggregate(&mut store, i).is_ok());

    let i = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap(),
        vec![
            Operation::Update {
                old: Record::new(
                    None,
                    vec![
                        Field::String("Milan".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(20)
                    ]
                ),
                new: Record::new(
                    None,
                    vec![
                        Field::String("Milan".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(10)
                    ]
                )
            },
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::String("Brescia".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(10)
                    ]
                )
            }
        ]
    );

    let i = Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap(),
        vec![
            Operation::Delete {
                old: Record::new(
                    None,
                    vec![
                        Field::String("Milan".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(10)
                    ]
                )
            },
            Operation::Update {
                old: Record::new(
                    None,
                    vec![
                        Field::String("Brescia".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(10)
                    ]
                ),
                new: Record::new(
                    None,
                    vec![
                        Field::String("Brescia".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(20)
                    ]
                )
            }
        ]
    );
}

#[test]
fn perf_test() {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let ss = Arc::new(LmdbStateStoreManager::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        1024 * 1024 * 1024 * 10,
        20_000,
    ));
    let mut store = ss.init_state_store("test".to_string()).unwrap();

    let rules = vec![
        FieldRule::Dimension(0, true, Some("City".to_string())),
        FieldRule::Dimension(1, true, Some("Region".to_string())),
        FieldRule::Dimension(2, true, Some("Country".to_string())),
        FieldRule::Measure(
            3,
            Box::new(IntegerSumAggregator::new()),
            true,
            Some("Sum".to_string()),
        ),
    ];

    let agg = AggregationProcessor::new(rules);

    for _i in 0..1_000_000 {
        let op = Operation::Insert {
            new: Record::new(
                None,
                vec![
                    Field::String(format!("Milan{}", 1).to_string()),
                    Field::String("Italy".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                ],
            ),
        };

        assert!(agg.aggregate(store.as_mut(), op).is_ok());
    }
}
