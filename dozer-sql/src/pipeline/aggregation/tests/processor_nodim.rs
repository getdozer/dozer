use crate::pipeline::aggregation::processor::{AggregationProcessor, FieldRule};
use crate::pipeline::aggregation::sum::IntegerSumAggregator;
use crate::pipeline::aggregation::tests::schema::{gen_in_data, get_input_schema};
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::Processor;
use dozer_core::state::memory::MemoryStateStore;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::collections::HashMap;

#[test]
fn test_insert_update_delete() {
    let mut store = MemoryStateStore::new();
    let rules = vec![FieldRule::Measure(
        "People".to_string(),
        Box::new(IntegerSumAggregator::new()),
        true,
        Some("Total".to_string()),
    )];

    let mut agg = AggregationProcessor::new(rules);

    let exp_schema = Schema::empty()
        .field(
            FieldDefinition::new("Total".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .clone();

    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());

    let output_schema = agg
        .update_schema(DEFAULT_PORT_HANDLE, &input_schemas)
        .unwrap_or_else(|_e| panic!("Cannot get schema"));

    assert_eq!(exp_schema, output_schema);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Insert {
        new: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(10)]),
        new: Record::new(None, vec![Field::Int(20)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 10),
        new: gen_in_data("Brescia", "Lombardy", "Italy", 20),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(20)]),
        new: Record::new(None, vec![Field::Int(30)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 20),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(30)]),
        new: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Delete {
        old: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);
}
