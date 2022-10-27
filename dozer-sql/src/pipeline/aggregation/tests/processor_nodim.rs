use crate::pipeline::aggregation::processor::{AggregationProcessor, FieldRule};
use crate::pipeline::aggregation::sum::IntegerSumAggregator;
use crate::pipeline::aggregation::tests::schema::{gen_in_data, get_input_schema};
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::chk;
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::Processor;
use dozer_types::test_helper::get_temp_dir;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use rocksdb::{Options, WriteBatch, DB};
use std::collections::HashMap;

#[test]
fn test_insert_update_delete() {
    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 10);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);

    let db = chk!(DB::open(&opts, get_temp_dir()));

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

    let output_schema = chk!(agg.update_schema(DEFAULT_PORT_HANDLE, &input_schemas));

    assert_eq!(exp_schema, output_schema);

    let inp = Operation::Insert {
        new: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Insert {
        new: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Insert {
        new: gen_in_data("Brescia", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(10)]),
        new: Record::new(None, vec![Field::Int(20)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Update {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 10),
        new: gen_in_data("Brescia", "Lombardy", "Italy", 20),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(20)]),
        new: Record::new(None, vec![Field::Int(30)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Brescia", "Lombardy", "Italy", 20),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Update {
        old: Record::new(None, vec![Field::Int(30)]),
        new: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);

    let inp = Operation::Delete {
        old: gen_in_data("Milan", "Lombardy", "Italy", 10),
    };
    let mut b = WriteBatch::default();
    let out = chk!(agg.aggregate(&db, &mut b, inp));
    chk!(db.write(b));
    let exp = vec![Operation::Delete {
        old: Record::new(None, vec![Field::Int(10)]),
    }];
    assert_eq!(out, exp);
}
