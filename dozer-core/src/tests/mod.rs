use std::sync::{atomic::AtomicBool, Arc};

use dozer_log::tokio::runtime::{self, Runtime};

use crate::{
    checkpoint::create_checkpoint_for_test, errors::ExecutionError, executor::DagExecutor, Dag,
};

mod app;
mod checkpoint_ns;
mod dag_base_create_errors;
mod dag_base_errors;
mod dag_base_run;
mod dag_ports;
mod dag_schemas;
pub mod processors;
pub mod sinks;
pub mod sources;

fn create_test_runtime() -> Arc<Runtime> {
    Arc::new(
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
}

fn run_dag(dag: Dag) -> Result<(), ExecutionError> {
    let runtime = create_test_runtime();
    let runtime_clone = runtime.clone();
    let handle = runtime.block_on(async move {
        let (_temp_dir, checkpoint) = create_checkpoint_for_test().await;
        DagExecutor::new(dag, checkpoint, Default::default())
            .await?
            .start(
                Arc::new(AtomicBool::new(true)),
                Default::default(),
                runtime_clone,
            )
            .await
    })?;
    handle.join()
}
