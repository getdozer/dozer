use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::mt_executor::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use crate::dag::tests::processors::{TestProcessorFactory, TestSinkFactory, TestSourceFactory};
use dozer_types::chk;
use dozer_types::test_helper::{get_temp_dir, init_logger};
use log::info;
use rocksdb::{Options, DB};
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

#[test]
fn test_run_dag() {
    init_logger();
    info!("Running test_run_dag");

    let src = TestSourceFactory::new(1, vec![DEFAULT_PORT_HANDLE]);
    let proc = TestProcessorFactory::new(1, vec![DEFAULT_PORT_HANDLE], vec![DEFAULT_PORT_HANDLE]);
    let sink = TestSinkFactory::new(1, vec![DEFAULT_PORT_HANDLE]);

    let mut dag = Dag::new();

    dag.add_node(NodeType::Source(Box::new(src)), 1.to_string());
    dag.add_node(NodeType::Processor(Box::new(proc)), 2.to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), 3.to_string());

    let src_to_proc1 = dag.connect(
        Endpoint::new(1.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(2.to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(src_to_proc1.is_ok());

    let proc1_to_sink = dag.connect(
        Endpoint::new(2.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(3.to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(proc1_to_sink.is_ok());

    let tmp_dir = get_temp_dir();

    let exec = MultiThreadedDagExecutor::new(100000);

    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 100);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);
    opts.set_allow_mmap_reads(true);

    let db = chk!(DB::open(&opts, tmp_dir));

    assert!(exec.start(dag, db).is_ok());
}
