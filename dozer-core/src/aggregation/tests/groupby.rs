use std::fs;
use std::path::Path;
use dozer_types::types::{Field, Operation, Record};
use crate::aggregation::groupby::{FieldRule, SizedAggregationDataset};
use crate::aggregation::sum::IntegerSumAggregator;
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;


#[test]
fn test_insert_update_delete() {

    let mut store = MemoryStateStore::new();

    let agg = SizedAggregationDataset::new(
        0x02_u16, &mut store, None,
        vec![
            FieldRule::Dimension(0), // City
            FieldRule::Dimension(1), // Country
            FieldRule::Measure(Box::new(IntegerSumAggregator::new(3))) // People
        ]
    ).unwrap();

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap()[0], Operation::Insert {
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(10)
        ])
    });

    // Insert another 10
    let i = Operation::Insert {
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap()[0], Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(10)
        ]),
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(20)
        ])
    });

    // update from 10 to 5
    let i = Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ]),
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(5)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap()[0], Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(20)
        ]),
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(15)
        ])
    });


    // Delete 5
    let i = Operation::Delete {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(5)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap()[0], Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(15)
        ]),
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(10)
        ])
    });

    // Delete last 10
    let i = Operation::Delete {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap()[0], Operation::Delete {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::Int(10)
        ])
    });
    println!("ciao")


}

#[test]
fn test_insert_update_change_dims() {

    let mut store = MemoryStateStore::new();

    let agg = SizedAggregationDataset::new(
        0x02_u16, &mut store, None,
        vec![
            FieldRule::Dimension(0), // City
            FieldRule::Dimension(1), // Country
            FieldRule::Measure(Box::new(IntegerSumAggregator::new(3))) // People
        ]
    ).unwrap();

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    assert!(agg.aggregate(&mut store, i).is_ok());

    // Insert 10
    let i = Operation::Insert {
        new: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    assert!(agg.aggregate(&mut store, i).is_ok());


    let i = Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ]),
        new: Record::new(None, vec![
            Field::String("Brescia".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap(), vec![
        Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ]),
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        },
        Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        }
    ]);


    let i = Operation::Update {
        old: Record::new(None, vec![
            Field::String("Milan".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ]),
        new: Record::new(None, vec![
            Field::String("Brescia".to_string()),
            Field::String("Lombardy".to_string()),
            Field::String("Italy".to_string()),
            Field::Int(10)
        ])
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(o.unwrap(), vec![
        Operation::Delete {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        },
        Operation::Update {
            old: Record::new(None, vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(None, vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ])
        }
    ]);


}


#[test]
fn perf_test() {

    fs::create_dir(".data").is_ok();

    let sm = LmdbStateStoreManager::new(
        ".data".to_string(),
        1024*1024*1024*10,
        20_000
    );
    let ss = sm.unwrap();
    let mut store = ss.init_state_store("test".to_string()).unwrap();

    let agg = SizedAggregationDataset::new(
        0x02_u16, store.as_mut(), None,
        vec![
            FieldRule::Dimension(0), // City
            FieldRule::Dimension(1), // Region
            FieldRule::Dimension(2), // Country
            FieldRule::Measure(Box::new(IntegerSumAggregator::new(3)))
        ]
    ).unwrap();


    for _i in 0..1000000 {

        let op = Operation::Insert {
            new: Record::new(None, vec![
                Field::String(format!("Milan{}", 1).to_string()),
                Field::String("Italy".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10),
                Field::Int(20),
                Field::Int(20),
                Field::Int(20),
                Field::Int(20),
                Field::Int(20)
            ])
        };

        assert!(agg.aggregate(store.as_mut(), op).is_ok());

    }
    fs::remove_dir_all(".data").is_ok();



}

