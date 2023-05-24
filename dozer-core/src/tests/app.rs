use crate::app::{App, AppPipeline, PipelineEntryPoint};
use crate::appsource::{AppSource, AppSourceId, AppSourceManager};
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{OutputPortDef, PortHandle, Source, SourceFactory};
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
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::types::Schema;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct NoneContext {}

#[derive(Debug)]
struct NoneSourceFactory {}
impl SourceFactory<NoneContext> for NoneSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<(Schema, NoneContext), BoxedError> {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        todo!()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        todo!()
    }
}

#[test]
fn test_apps_sorce_smanager_connection_exists() {
    let mut asm = AppSourceManager::new();
    let app_src = AppSource::new(
        "conn1".to_string(),
        Arc::new(NoneSourceFactory {}),
        vec![("table1".to_string(), 1_u16)].into_iter().collect(),
    );
    let _r = asm.add(app_src);
    let app_src = AppSource::new(
        "conn1".to_string(),
        Arc::new(NoneSourceFactory {}),
        vec![("table2".to_string(), 1_u16)].into_iter().collect(),
    );
    let r = asm.add(app_src);
    assert!(r.is_err());
}

#[test]
fn test_apps_sorce_smanager_lookup() {
    let mut asm = AppSourceManager::new();
    let app_src = AppSource::new(
        "conn1".to_string(),
        Arc::new(NoneSourceFactory {}),
        vec![("table1".to_string(), 1_u16)].into_iter().collect(),
    );
    asm.add(app_src).unwrap();

    let r = asm
        .get(vec![AppSourceId::new("table1".to_string(), None)])
        .unwrap();
    assert_eq!(r[0].source.connection, "conn1");
    assert_eq!(
        r[0].mappings
            .get(&AppSourceId::new("table1".to_string(), None))
            .unwrap(),
        &1_u16
    );

    let r = asm.get(vec![AppSourceId::new(
        "table1".to_string(),
        Some("No connection".to_string()),
    )]);
    assert!(r.is_err());

    let r = asm
        .get(vec![AppSourceId::new(
            "table1".to_string(),
            Some("conn1".to_string()),
        )])
        .unwrap();
    assert_eq!(r[0].source.connection, "conn1");
    assert_eq!(
        r[0].mappings
            .get(&AppSourceId::new(
                "table1".to_string(),
                Some("conn1".to_string())
            ))
            .unwrap(),
        &1_u16
    );

    // Insert same table name
    let app_src = AppSource::new(
        "conn2".to_string(),
        Arc::new(NoneSourceFactory {}),
        vec![("table1".to_string(), 2_u16)].into_iter().collect(),
    );
    asm.add(app_src).unwrap();

    let r = asm.get(vec![AppSourceId::new("table1".to_string(), None)]);
    assert!(r.is_err());

    let r = asm
        .get(vec![
            AppSourceId::new("table1".to_string(), Some("conn1".to_string())),
            AppSourceId::new("table1".to_string(), Some("conn2".to_string())),
        ])
        .unwrap();

    let conn1 = r.iter().find(|e| e.source.connection == "conn1");
    assert!(conn1.is_some());
    let conn2 = r.iter().find(|e| e.source.connection == "conn2");
    assert!(conn2.is_some());

    assert_eq!(
        conn1
            .unwrap()
            .mappings
            .get(&AppSourceId::new(
                "table1".to_string(),
                Some("conn1".to_string())
            ))
            .unwrap(),
        &1_u16
    );
    assert_eq!(
        conn2
            .unwrap()
            .mappings
            .get(&AppSourceId::new(
                "table1".to_string(),
                Some("conn2".to_string())
            ))
            .unwrap(),
        &2_u16
    );
}

#[test]
fn test_apps_source_manager_lookup_multiple_ports() {
    let mut asm = AppSourceManager::new();
    let app_src = AppSource::new(
        "conn1".to_string(),
        Arc::new(NoneSourceFactory {}),
        vec![("table1".to_string(), 1_u16), ("table2".to_string(), 2_u16)]
            .into_iter()
            .collect(),
    );
    asm.add(app_src).unwrap();

    let _r = asm.get(vec![
        AppSourceId::new("table1".to_string(), None),
        AppSourceId::new("table2".to_string(), None),
    ]);

    let r = asm
        .get(vec![
            AppSourceId::new("table1".to_string(), None),
            AppSourceId::new("table2".to_string(), None),
        ])
        .unwrap();

    assert_eq!(r[0].source.connection, "conn1");
    assert_eq!(
        r[0].mappings
            .get(&AppSourceId::new("table1".to_string(), None))
            .unwrap(),
        &1_u16
    );
    assert_eq!(
        r[0].mappings
            .get(&AppSourceId::new("table2".to_string(), None))
            .unwrap(),
        &2_u16
    );
}

#[test]
fn test_app_dag() {
    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "postgres".to_string(),
        Arc::new(DualPortGeneratorSourceFactory::new(
            10_000,
            latch.clone(),
            true,
        )),
        vec![
            (
                "users".to_string(),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
            ),
            (
                "transactions".to_string(),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
            ),
        ]
        .into_iter()
        .collect(),
    ))
    .unwrap();

    asm.add(AppSource::new(
        "snowflake".to_string(),
        Arc::new(GeneratorSourceFactory::new(10_000, latch.clone(), true)),
        vec![("users".to_string(), GENERATOR_SOURCE_OUTPUT_PORT)]
            .into_iter()
            .collect(),
    ))
    .unwrap();

    let mut app = App::new(asm);

    let mut p1 = AppPipeline::new();
    p1.add_processor(
        Arc::new(NoopJoinProcessorFactory {}),
        "join",
        vec![
            PipelineEntryPoint::new(
                AppSourceId::new("users".to_string(), Some("postgres".to_string())),
                NOOP_JOIN_LEFT_INPUT_PORT,
            ),
            PipelineEntryPoint::new(
                AppSourceId::new("transactions".to_string(), None),
                NOOP_JOIN_RIGHT_INPUT_PORT,
            ),
        ],
    );
    p1.add_sink(
        Arc::new(CountingSinkFactory::new(20_000, latch.clone())),
        "sink",
    );
    p1.connect_nodes("join", None, "sink", Some(COUNTING_SINK_INPUT_PORT), true)
        .unwrap();

    app.add_pipeline(p1);

    let mut p2 = AppPipeline::new();
    p2.add_processor(
        Arc::new(NoopJoinProcessorFactory {}),
        "join",
        vec![
            PipelineEntryPoint::new(
                AppSourceId::new("users".to_string(), Some("snowflake".to_string())),
                NOOP_JOIN_LEFT_INPUT_PORT,
            ),
            PipelineEntryPoint::new(
                AppSourceId::new("transactions".to_string(), None),
                NOOP_JOIN_RIGHT_INPUT_PORT,
            ),
        ],
    );
    p2.add_sink(Arc::new(CountingSinkFactory::new(20_000, latch)), "sink");
    p2.connect_nodes("join", None, "sink", Some(COUNTING_SINK_INPUT_PORT), true)
        .unwrap();

    app.add_pipeline(p2);

    let dag = app.get_dag().unwrap();
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

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}
