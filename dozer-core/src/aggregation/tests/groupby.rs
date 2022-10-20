use crate::aggregation::groupby::AggregationProcessor;
use crate::aggregation::tests::schema::{
    gen_in_data, gen_out_data, get_aggregator_rules, get_expected_schema, get_input_schema,
};
use crate::dag::dag::PortHandle;
use crate::dag::mt_executor::DEFAULT_PORT_HANDLE;
use crate::dag::node::Processor;
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;
use crate::state::{StateStoreOptions, StateStoresManager};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

#[test]
fn test_schema_building() {
    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let output_schema = agg
        .update_schema(DEFAULT_PORT_HANDLE, &input_schemas)
        .unwrap_or_else(|_e| panic!("Cannot get schema"));
    assert_eq!(output_schema, get_expected_schema());
}

#[test]
fn test_insert_update_delete() {
    let mut store = MemoryStateStore::new();

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let _output_schema = agg
        .update_schema(DEFAULT_PORT_HANDLE, &input_schemas)
        .unwrap_or_else(|_e| panic!("Cannot get schema"));

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 10),
        new: gen_out_data("Milan", "Italy", 20),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
        new: gen_in_data("Milan", "Lombardy", "Italy", 5),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 20),
        new: gen_out_data("Milan", "Italy", 15),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 5),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 15),
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Brescia", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Delete {
        old: gen_out_data("Brescia", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Delete {
        old: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_insert_update_change_dims() {
    let mut store = MemoryStateStore::new();

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let _output_schema = agg
        .update_schema(DEFAULT_PORT_HANDLE, &input_schemas)
        .unwrap_or_else(|_e| panic!("Cannot get schema"));

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 10),
        new: gen_out_data("Milan", "Italy", 20),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![
        Operation::Update {
            old: gen_out_data("Milan", "Italy", 20),
            new: gen_out_data("Milan", "Italy", 10),
        },
        Operation::Insert {
            new: gen_out_data("Brescia", "Italy", 10),
        },
    ];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let out = agg
        .aggregate(&mut store, inp)
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));
    let exp = vec![
        Operation::Delete {
            old: gen_out_data("Milan", "Italy", 10),
        },
        Operation::Update {
            old: gen_out_data("Brescia", "Italy", 10),
            new: gen_out_data("Brescia", "Italy", 20),
        },
    ];
    assert_eq!(out, exp);
}

#[test]
fn bench_aggregator() {
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
    let mut store = ss
        .init_state_store("test".to_string(), StateStoreOptions::default())
        .unwrap();

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let output_schema = agg
        .update_schema(DEFAULT_PORT_HANDLE, &input_schemas)
        .unwrap_or_else(|_e| panic!("Cannot get schema"));
    assert_eq!(output_schema, get_expected_schema());

    for i in 0..100_000 {
        let op = Operation::Insert {
            new: gen_in_data(
                format!("Milan{}", i % 10000).as_str(),
                "Lombardy",
                "Italy",
                1,
            ),
        };
        assert!(agg.aggregate(store.as_mut(), op).is_ok());
    }
}
