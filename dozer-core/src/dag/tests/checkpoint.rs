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
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Barrier};

use tempdir::TempDir;

#[test]
fn test_checkpoint_consistency() {
    //  dozer_tracing::init_telemetry(false).unwrap();
    let mut dag = Dag::new();
    let latch = Arc::new(Barrier::new(3));

    const SRC1_MSG_COUNT: u64 = 50_000;
    const SRC2_MSG_COUNT: u64 = 25_000;

    const SRC1_HANDLE_ID: &str = "SRC1";
    const SRC2_HANDLE_ID: &str = "SRC2";
    const PROC_HANDLE_ID: &str = "PROC";
    const SINK_HANDLE_ID: &str = "SINK";

    let source1_handle = NodeHandle::new(Some(1), SRC1_HANDLE_ID.to_string());
    let source2_handle = NodeHandle::new(Some(1), SRC2_HANDLE_ID.to_string());
    let proc_handle = NodeHandle::new(Some(1), PROC_HANDLE_ID.to_string());
    let sink_handle = NodeHandle::new(Some(1), SINK_HANDLE_ID.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            SRC1_MSG_COUNT,
            latch.clone(),
            true,
        ))),
        source1_handle.clone(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            SRC2_MSG_COUNT,
            latch.clone(),
            true,
        ))),
        source2_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(
            SRC1_MSG_COUNT + SRC2_MSG_COUNT,
            latch,
        ))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle.clone(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(SRC1_MSG_COUNT, 0)),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(SRC2_MSG_COUNT, 0)),
    }

    LmdbEnvironmentManager::remove(tmp_dir.path(), format!("{}", proc_handle).as_str());
    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    let mut expected: HashMap<(u64, u64), Vec<NodeHandle>> = HashMap::new();
    expected.insert(
        (SRC1_MSG_COUNT, 0),
        vec![source1_handle.clone(), sink_handle.clone()],
    );
    expected.insert((0_u64, 0), vec![proc_handle.clone()]);
    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }

    let mut expected: HashMap<(u64, u64), Vec<NodeHandle>> = HashMap::new();
    expected.insert(
        (SRC2_MSG_COUNT, 0),
        vec![source2_handle.clone(), sink_handle],
    );
    expected.insert((0_u64, 0), vec![proc_handle]);
    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }
}

#[test]
fn test_checkpoint_consistency_resume() {
    //   dozer_tracing::init_telemetry(false).unwrap();
    let mut dag = Dag::new();
    let latch = Arc::new(Barrier::new(3));

    let source1_handle = NodeHandle::new(Some(1), 1.to_string());
    let source2_handle = NodeHandle::new(Some(1), 2.to_string());
    let proc_handle = NodeHandle::new(Some(1), 3.to_string());
    let sink_handle = NodeHandle::new(Some(1), 4.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            25_000,
            latch.clone(),
            true,
        ))),
        source1_handle.clone(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            50_000,
            latch.clone(),
            true,
        ))),
        source2_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(50_000 + 25_000, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(25_000, 0)),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(50_000, 0)),
    }

    let mut dag = Dag::new();
    let latch = Arc::new(Barrier::new(3));

    let source1_handle = NodeHandle::new(Some(1), 1.to_string());
    let source2_handle = NodeHandle::new(Some(1), 2.to_string());
    let proc_handle = NodeHandle::new(Some(1), 3.to_string());
    let sink_handle = NodeHandle::new(Some(1), 4.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            25_000,
            latch.clone(),
            true,
        ))),
        source1_handle.clone(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            50_000,
            latch.clone(),
            true,
        ))),
        source2_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(50_000 + 25_000, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(50_000, 0)),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r, &(100_000, 0)),
    }
}
