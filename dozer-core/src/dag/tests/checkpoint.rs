use crate::chk;
use crate::dag::dag::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use crate::dag::dag_metadata::{Consistency, DagMetadataManager};
use crate::dag::epoch::OpIdentifier;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::NodeHandle;
use crate::dag::tests::dag_base_run::NoopJoinProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tempdir::TempDir;

#[test]
fn test_checkpoint_consistency() {
    //  dozer_tracing::init_telemetry(false).unwrap();
    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    const SRC1_MSG_COUNT: u64 = 5000;
    const SRC2_MSG_COUNT: u64 = 5000;

    const SRC1_HANDLE_ID: &str = "SRC1";
    const SRC2_HANDLE_ID: &str = "SRC2";
    const PROC_HANDLE_ID: &str = "PROC";
    const SINK_HANDLE_ID: &str = "SINK";

    let source1_handle = NodeHandle::new(Some(1), SRC1_HANDLE_ID.to_string());
    let source2_handle = NodeHandle::new(Some(1), SRC2_HANDLE_ID.to_string());
    let proc_handle = NodeHandle::new(Some(1), PROC_HANDLE_ID.to_string());
    let sink_handle = NodeHandle::new(Some(1), SINK_HANDLE_ID.to_string());

    dag.add_source(
        source1_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(
            SRC1_MSG_COUNT,
            latch.clone(),
            true,
        )),
    );
    dag.add_source(
        source2_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(
            SRC2_MSG_COUNT,
            latch.clone(),
            true,
        )),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(
            SRC1_MSG_COUNT + SRC2_MSG_COUNT,
            latch,
        )),
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
        Consistency::FullyConsistent(r) => {
            assert_eq!(r.unwrap(), OpIdentifier::new(SRC1_MSG_COUNT, 0))
        }
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => {
            assert_eq!(r.unwrap(), OpIdentifier::new(SRC2_MSG_COUNT, 0))
        }
    }

    LmdbEnvironmentManager::remove(tmp_dir.path(), format!("{proc_handle}").as_str());
    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();

    let mut expected = HashMap::new();
    expected.insert(
        Some(OpIdentifier::new(SRC1_MSG_COUNT, 0)),
        vec![source1_handle.clone(), sink_handle.clone()],
    );
    expected.insert(None, vec![proc_handle.clone()]);
    match c.get(&source1_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }

    let mut expected = HashMap::new();
    expected.insert(
        Some(OpIdentifier::new(SRC2_MSG_COUNT, 0)),
        vec![source2_handle.clone(), sink_handle],
    );
    expected.insert(None, vec![proc_handle]);
    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(r) => assert_eq!(r, &expected),
        Consistency::FullyConsistent(_r) => panic!("Wrong consistency"),
    }
}

#[test]
fn test_checkpoint_consistency_resume() {
    //   dozer_tracing::init_telemetry(false).unwrap();
    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(Some(1), 1.to_string());
    let source2_handle = NodeHandle::new(Some(1), 2.to_string());
    let proc_handle = NodeHandle::new(Some(1), 3.to_string());
    let sink_handle = NodeHandle::new(Some(1), 4.to_string());

    dag.add_source(
        source1_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(25_000, latch.clone(), true)),
    );
    dag.add_source(
        source2_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(50_000, latch.clone(), true)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(50_000 + 25_000, latch)),
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
        Consistency::FullyConsistent(r) => assert_eq!(r.unwrap(), OpIdentifier::new(25_000, 0)),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r.unwrap(), OpIdentifier::new(50_000, 0)),
    }

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(Some(1), 1.to_string());
    let source2_handle = NodeHandle::new(Some(1), 2.to_string());
    let proc_handle = NodeHandle::new(Some(1), 3.to_string());
    let sink_handle = NodeHandle::new(Some(1), 4.to_string());

    dag.add_source(
        source1_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(25_000, latch.clone(), true)),
    );
    dag.add_source(
        source2_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(50_000, latch.clone(), true)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(50_000 + 25_000, latch)),
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
        Consistency::FullyConsistent(r) => assert_eq!(r.unwrap(), OpIdentifier::new(50_000, 0)),
    }

    match c.get(&source2_handle).unwrap() {
        Consistency::PartiallyConsistent(_r) => panic!("Wrong consistency"),
        Consistency::FullyConsistent(r) => assert_eq!(r.unwrap(), OpIdentifier::new(100_000, 0)),
    }
}
