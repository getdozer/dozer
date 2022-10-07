use crate::aggregation::groupby::{AggregationProcessor, AggregationProcessorFactory, FieldRule};
use crate::aggregation::sum::IntegerSumAggregator;
use crate::dag::node::{NodeOperation, ProcessorFactory};
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;
use dozer_types::types::{Field, Operation, Record};
use std::fs;
use std::path::Path;

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
    let i = NodeOperation::Insert {
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
        NodeOperation::Insert {
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
    let i = NodeOperation::Insert {
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
        NodeOperation::Update {
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
    let i = NodeOperation::Update {
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
        NodeOperation::Update {
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
    let i = NodeOperation::Delete {
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
        NodeOperation::Update {
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
    let i = NodeOperation::Delete {
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
        NodeOperation::Delete {
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
    println!("ciao")
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
    let i = NodeOperation::Insert {
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
    let i = NodeOperation::Insert {
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

    let i = NodeOperation::Update {
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
            NodeOperation::Update {
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
            NodeOperation::Insert {
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

    let i = NodeOperation::Update {
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
            NodeOperation::Delete {
                old: Record::new(
                    None,
                    vec![
                        Field::String("Milan".to_string()),
                        Field::String("Lombardy".to_string()),
                        Field::Int(10)
                    ]
                )
            },
            NodeOperation::Update {
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
    fs::create_dir(".data").is_ok();

    let sm = LmdbStateStoreManager::new(".data".to_string(), 1024 * 1024 * 1024 * 10, 20_000);
    let ss = sm.unwrap();
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
        let op = NodeOperation::Insert {
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
    fs::remove_dir_all(".data").is_ok();
}
