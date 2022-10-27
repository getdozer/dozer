use crate::pipeline::aggregation::processor::AggregationProcessor;
use crate::pipeline::aggregation::tests::schema::{
    gen_in_data, gen_out_data, get_aggregator_rules, get_expected_schema, get_input_schema,
};
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::chk;
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::Processor;
use dozer_types::test_helper::get_temp_dir;
use dozer_types::types::{Operation, Schema};
use rocksdb::{Options, WriteBatch, DB};
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
    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 10);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);

    let db = chk!(DB::open(&opts, get_temp_dir()));

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let _output_schema = chk!(agg.update_schema(DEFAULT_PORT_HANDLE, &input_schemas));

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 10),
        new: gen_out_data("Milan", "Italy", 20),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
        new: gen_in_data("Milan", "Lombardy", "Italy", 5),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 20),
        new: gen_out_data("Milan", "Italy", 15),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 5),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 15),
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Brescia", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Delete {
        old: gen_out_data("Brescia", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Delete {
        old: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_insert_update_change_dims() {
    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 10);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);

    let db = chk!(DB::open(&opts, get_temp_dir()));

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let _output_schema = chk!(agg.update_schema(DEFAULT_PORT_HANDLE, &input_schemas));

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Insert {
        new: gen_out_data("Milan", "Italy", 10),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: gen_out_data("Milan", "Italy", 10),
        new: gen_out_data("Milan", "Italy", 20),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
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
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
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
    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 10);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);

    let db = chk!(DB::open(&opts, get_temp_dir()));

    let mut agg = AggregationProcessor::new(get_aggregator_rules());
    let mut input_schemas = HashMap::<PortHandle, Schema>::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, get_input_schema());
    let output_schema = chk!(agg.update_schema(DEFAULT_PORT_HANDLE, &input_schemas));
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
        let mut batch = WriteBatch::default();
        assert!(agg.aggregate(&db, &mut batch, op).is_ok());
        chk!(db.write(batch));
    }
}
