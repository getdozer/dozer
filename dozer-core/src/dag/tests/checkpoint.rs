use crate::chk;
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::dag_metadata::{Consistency, DagMetadataManager};
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory};
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::dag_base_run::{NoopJoinProcessorFactory, NoopProcessorFactory};
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use dozer_types::types::Schema;
use fp_rust::sync::CountDownLatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

#[test]
fn test_checpoint_consistency() {
    init_log4rs();
    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            25_000,
            latch.clone(),
            true,
        ))),
        "source1".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            50_000,
            latch.clone(),
            true,
        ))),
        "source2".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        "proc".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(
            25_000 + 50_000,
            latch.clone(),
        ))),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("source1".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new("source2".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new("proc".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    match c.get("source1").unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &25_000),
    }

    match c.get("source2").unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &50_000),
    }

    LmdbEnvironmentManager::remove(tmp_dir.path(), "proc");
    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    let mut expected: HashMap<u64, Vec<NodeHandle>> = HashMap::new();
    expected.insert(25000_u64, vec!["source1".to_string(), "sink".to_string()]);
    expected.insert(0_u64, vec!["proc".to_string()]);
    match c.get("source1").unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }

    let mut expected: HashMap<u64, Vec<NodeHandle>> = HashMap::new();
    expected.insert(50000_u64, vec!["source2".to_string(), "sink".to_string()]);
    expected.insert(0_u64, vec!["proc".to_string()]);
    match c.get("source2").unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }
}
