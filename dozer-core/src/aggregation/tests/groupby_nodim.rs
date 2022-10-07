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

    let rules = vec![FieldRule::Measure(
        3,
        Box::new(IntegerSumAggregator::new()),
        true,
        Some("Sum".to_string()),
    )];
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
            new: Record::new(None, vec![Field::Int(10)])
        }
    );

    // Insert 10
    let i = NodeOperation::Insert {
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
        o.unwrap()[0],
        NodeOperation::Update {
            old: Record::new(None, vec![Field::Int(10)]),
            new: Record::new(None, vec![Field::Int(20)])
        }
    );

    // Update to 20
    let i = NodeOperation::Update {
        old: Record::new(
            None,
            vec![
                Field::String("Brescia".to_string()),
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
                Field::Int(20),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        NodeOperation::Update {
            old: Record::new(None, vec![Field::Int(20)]),
            new: Record::new(None, vec![Field::Int(30)])
        }
    );

    // Delete 2nd
    let i = NodeOperation::Delete {
        old: Record::new(
            None,
            vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(20),
            ],
        ),
    };
    let o = agg.aggregate(&mut store, i);
    assert_eq!(
        o.unwrap()[0],
        NodeOperation::Update {
            old: Record::new(None, vec![Field::Int(30)]),
            new: Record::new(None, vec![Field::Int(10)])
        }
    );

    // Delete 1st
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
            old: Record::new(None, vec![Field::Int(10)])
        }
    );
}
