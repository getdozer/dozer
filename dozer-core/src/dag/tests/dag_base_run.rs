use crate::chk;
use crate::dag::dag::{Dag, NodeType};
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::tests::sources::{GeneratorSource, GeneratorSourceFactory};
use std::sync::Arc;
use tempdir::TempDir;

#[test]
fn test_run_dag() {
    let mut dag = Dag::new();

    let src_factory = Arc::new(GeneratorSourceFactory::new(100));
    dag.add_node(NodeType::Source(src_factory), "src".to_string());

    let tmp_dir = chk!(TempDir::new("test"));
    let executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));
}
