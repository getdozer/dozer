use crate::chk;
use crate::dag_metadata::DagMetadata;
use crate::dag_schemas::DagSchemas;
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::NodeHandle;
use crate::tests::dag_base_run::NoopJoinProcessorFactory;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_storage::lmdb_storage::LmdbEnvironmentManager;

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
    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();

    let dag_schemas = DagSchemas::new(&dag).unwrap();

    let dag_metadata = DagMetadata::new(&dag_schemas, tmp_dir.path().to_path_buf()).unwrap();
    assert!(dag_metadata.check_consistency());

    LmdbEnvironmentManager::remove(tmp_dir.path(), format!("{proc_handle}").as_str());
    let dag_metadata = DagMetadata::new(&dag_schemas, tmp_dir.path().to_path_buf()).unwrap();
    assert!(!dag_metadata.check_consistency());
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
    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();

    let dag_schemas = DagSchemas::new(&dag).unwrap();

    let dag_metadata = DagMetadata::new(&dag_schemas, tmp_dir.path().to_path_buf()).unwrap();
    assert!(dag_metadata.check_consistency());

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

    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();

    let dag_schemas = DagSchemas::new(&dag).unwrap();

    let dag_metadata = DagMetadata::new(&dag_schemas, tmp_dir.path().to_path_buf()).unwrap();
    assert!(dag_metadata.check_consistency());
}
