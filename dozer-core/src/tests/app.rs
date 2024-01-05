use crate::app::{App, AppPipeline, PipelineEntryPoint};
use crate::appsource::{AppSourceManager, AppSourceMappings};
use crate::checkpoint::create_checkpoint_for_test;
use crate::executor::DagExecutor;
use crate::node::{OutputPortDef, PortHandle, Source, SourceFactory, SourceState};
use crate::tests::dag_base_run::{
    NoopJoinProcessorFactory, NOOP_JOIN_LEFT_INPUT_PORT, NOOP_JOIN_RIGHT_INPUT_PORT,
};
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{
    DualPortGeneratorSourceFactory, GeneratorSourceFactory,
    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
    GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::{Edge, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_log::tokio;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::types::Schema;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Debug)]
struct NoneSourceFactory {}
impl SourceFactory for NoneSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        todo!()
    }

    fn get_output_port_name(&self, _port: &PortHandle) -> String {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        todo!()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
        _last_checkpoint: SourceState,
    ) -> Result<Box<dyn Source>, BoxedError> {
        todo!()
    }
}

#[test]
fn test_apps_source_manager_connection_exists() {
    let mut asm = AppSourceManager::new();
    let _r = asm.add(
        Box::new(NoneSourceFactory {}),
        AppSourceMappings::new(
            "conn1".to_string(),
            vec![("table1".to_string(), 1_u16)].into_iter().collect(),
        ),
    );
    let r = asm.add(
        Box::new(NoneSourceFactory {}),
        AppSourceMappings::new(
            "conn1".to_string(),
            vec![("table2".to_string(), 1_u16)].into_iter().collect(),
        ),
    );
    assert!(r.is_err());
}

#[test]
fn test_apps_source_manager_lookup() {
    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(NoneSourceFactory {}),
        AppSourceMappings::new(
            "conn1".to_string(),
            vec![("table1".to_string(), 1_u16)].into_iter().collect(),
        ),
    )
    .unwrap();

    let r = asm.get_endpoint("table1").unwrap();
    assert_eq!(r.node.id, "conn1");
    assert_eq!(r.port, 1_u16);

    let r = asm.get_endpoint("Non-existent source");
    assert!(r.is_err());

    // Insert another source
    asm.add(
        Box::new(NoneSourceFactory {}),
        AppSourceMappings::new(
            "conn2".to_string(),
            vec![("table2".to_string(), 2_u16)].into_iter().collect(),
        ),
    )
    .unwrap();

    let r = asm.get_endpoint("table3");
    assert!(r.is_err());

    let r = asm.get_endpoint("table1").unwrap();
    assert_eq!(r.node.id, "conn1");
    assert_eq!(r.port, 1_u16);

    let r = asm.get_endpoint("table2").unwrap();
    assert_eq!(r.node.id, "conn2");
    assert_eq!(r.port, 2_u16);
}

#[test]
fn test_apps_source_manager_lookup_multiple_ports() {
    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(NoneSourceFactory {}),
        AppSourceMappings::new(
            "conn1".to_string(),
            vec![("table1".to_string(), 1_u16), ("table2".to_string(), 2_u16)]
                .into_iter()
                .collect(),
        ),
    )
    .unwrap();

    let r = asm.get_endpoint("table1").unwrap();
    assert_eq!(r.node.id, "conn1");
    assert_eq!(r.port, 1_u16);

    let r = asm.get_endpoint("table2").unwrap();
    assert_eq!(r.node.id, "conn1");
    assert_eq!(r.port, 2_u16);
}

#[tokio::test]
async fn test_app_dag() {
    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(DualPortGeneratorSourceFactory::new(
            10_000,
            latch.clone(),
            true,
        )),
        AppSourceMappings::new(
            "postgres".to_string(),
            vec![
                (
                    "users_postgres".to_string(),
                    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
                ),
                (
                    "transactions".to_string(),
                    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
                ),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();

    asm.add(
        Box::new(GeneratorSourceFactory::new(10_000, latch.clone(), true)),
        AppSourceMappings::new(
            "snowflake".to_string(),
            vec![("users_snowflake".to_string(), GENERATOR_SOURCE_OUTPUT_PORT)]
                .into_iter()
                .collect(),
        ),
    )
    .unwrap();

    let mut app = App::new(asm);

    let mut p1 = AppPipeline::new_with_default_flags();
    p1.add_processor(
        Box::new(NoopJoinProcessorFactory {}),
        "join",
        vec![
            PipelineEntryPoint::new("users_postgres".to_string(), NOOP_JOIN_LEFT_INPUT_PORT),
            PipelineEntryPoint::new("transactions".to_string(), NOOP_JOIN_RIGHT_INPUT_PORT),
        ],
    );
    p1.add_sink(
        Box::new(CountingSinkFactory::new(20_000, latch.clone())),
        "sink",
        None,
    );
    p1.connect_nodes(
        "join",
        DEFAULT_PORT_HANDLE,
        "sink",
        COUNTING_SINK_INPUT_PORT,
    );

    app.add_pipeline(p1);

    let mut p2 = AppPipeline::new_with_default_flags();
    p2.add_processor(
        Box::new(NoopJoinProcessorFactory {}),
        "join",
        vec![
            PipelineEntryPoint::new("users_snowflake".to_string(), NOOP_JOIN_LEFT_INPUT_PORT),
            PipelineEntryPoint::new("transactions".to_string(), NOOP_JOIN_RIGHT_INPUT_PORT),
        ],
    );
    p2.add_sink(
        Box::new(CountingSinkFactory::new(20_000, latch)),
        "sink",
        None,
    );
    p2.connect_nodes(
        "join",
        DEFAULT_PORT_HANDLE,
        "sink",
        COUNTING_SINK_INPUT_PORT,
    );

    app.add_pipeline(p2);

    let dag = app.into_dag().unwrap();
    let edges = dag.edge_handles();

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(None, "postgres".to_string()),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1
            ),
            Endpoint::new(
                NodeHandle::new(Some(1), "join".to_string()),
                NOOP_JOIN_LEFT_INPUT_PORT
            )
        )));

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(None, "postgres".to_string()),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2
            ),
            Endpoint::new(
                NodeHandle::new(Some(1), "join".to_string()),
                NOOP_JOIN_RIGHT_INPUT_PORT
            )
        )));

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(None, "snowflake".to_string()),
                GENERATOR_SOURCE_OUTPUT_PORT
            ),
            Endpoint::new(
                NodeHandle::new(Some(2), "join".to_string()),
                NOOP_JOIN_LEFT_INPUT_PORT
            )
        )));

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(None, "postgres".to_string()),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2
            ),
            Endpoint::new(
                NodeHandle::new(Some(2), "join".to_string()),
                NOOP_JOIN_RIGHT_INPUT_PORT
            )
        )));

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(Some(1), "join".to_string()),
                DEFAULT_PORT_HANDLE
            ),
            Endpoint::new(
                NodeHandle::new(Some(1), "sink".to_string()),
                COUNTING_SINK_INPUT_PORT
            )
        )));

    assert!(edges.iter().any(|e| *e
        == Edge::new(
            Endpoint::new(
                NodeHandle::new(Some(2), "join".to_string()),
                DEFAULT_PORT_HANDLE
            ),
            Endpoint::new(
                NodeHandle::new(Some(2), "sink".to_string()),
                COUNTING_SINK_INPUT_PORT
            )
        )));

    assert_eq!(edges.len(), 6);

    let (_temp_dir, checkpoint) = create_checkpoint_for_test().await;
    DagExecutor::new(dag, checkpoint, Default::default())
        .await
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)), Default::default())
        .await
        .unwrap()
        .join()
        .unwrap();
}
