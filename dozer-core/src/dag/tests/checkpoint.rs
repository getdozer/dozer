use crate::chk;
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::dag_metadata::{Consistency, DagMetadataManager};
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::NodeHandle;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::dag_base_run::NoopJoinProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;

use fp_rust::sync::CountDownLatch;
use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;

#[test]
fn test_checpoint_consistency() {
    init_log4rs();
    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    let source1_handle = NodeHandle::new(Some(1), 1);
    let source2_handle = NodeHandle::new(Some(1), 2);
    let proc_handle = NodeHandle::new(Some(1), 3);
    let sink_handle = NodeHandle::new(Some(1), 4);

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            25_000,
            latch.clone(),
            true,
        ))),
        source1_handle,
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            50_000,
            latch.clone(),
            true,
        ))),
        source2_handle,
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle,
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(50_000 + 25_000, latch))),
        sink_handle,
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle, 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle, 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &25_000),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &50_000),
    }

    LmdbEnvironmentManager::remove(tmp_dir.path(), format!("{}", proc_handle).as_str());
    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    let mut expected: HashMap<u64, Vec<NodeHandle>> = HashMap::new();
    expected.insert(25000_u64, vec![source1_handle, sink_handle]);
    expected.insert(0_u64, vec![proc_handle]);
    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }

    let mut expected: HashMap<u64, Vec<NodeHandle>> = HashMap::new();
    expected.insert(50000_u64, vec![source2_handle, sink_handle]);
    expected.insert(0_u64, vec![proc_handle]);
    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }
}
