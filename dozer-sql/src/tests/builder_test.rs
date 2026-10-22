use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::epoch::Epoch;
use dozer_core::event::EventHub;
use dozer_core::executor::DagExecutor;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::DateTime;
use dozer_types::errors::internal::BoxedError;
use dozer_types::log::debug;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::OpIdentifier;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::tonic::async_trait;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition, TableOperation,
};
use tokio::sync::mpsc::Sender;

use std::collections::HashMap;
use std::future::pending;

use crate::builder::statement_to_pipeline;
use crate::tests::utils::create_test_runtime;

/// Test Source
#[derive(Debug)]
pub struct TestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl TestSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect()
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("CustomerID"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Country"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Spending"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("timestamp"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone())
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        format!("port_{}", port)
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
        _state: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(TestSource {}))
    }
}

#[derive(Debug)]
pub struct TestSource {}

#[async_trait]
impl Source for TestSource {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        for _ in 0..10 {
            sender
                .send((
                    DEFAULT_PORT_HANDLE,
                    IngestionMessage::OperationEvent {
                        table_index: 0,
                        op: Operation::Insert {
                            new: Record::new(vec![
                                Field::Int(0),
                                Field::String("Italy".to_string()),
                                Field::Float(OrderedFloat(5.5)),
                                Field::Timestamp(
                                    DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap(),
                                ),
                            ]),
                        },
                        id: None,
                    },
                ))
                .await
                .unwrap();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>) -> Self {
        Self { input_ports }
    }
}

#[async_trait]
impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        "test".to_string()
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(TestSink {}))
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    fn type_name(&self) -> String {
        "test".to_string()
    }
}

#[derive(Debug)]
pub struct TestSink {}

impl Sink for TestSink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        println!("Sink: {:?}", op);
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}

#[test]
fn test_pipeline_builder() {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let runtime = create_test_runtime();
    let context = statement_to_pipeline(
        "SELECT t.Spending  \
        FROM TTL(TUMBLE(users, timestamp, '5 MINUTES'), timestamp, '1 MINUTE') t JOIN users u on t.CustomerID=u.CustomerID \
         WHERE t.Spending >= 1",
        &mut pipeline,
        Some("results".to_string()),
        vec![],
        runtime.clone()
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE])),
        AppSourceMappings::new(
            "mem".to_string(),
            vec![("users".to_string(), DEFAULT_PORT_HANDLE)]
                .into_iter()
                .collect(),
        ),
    )
    .unwrap();

    pipeline.add_sink(
        Box::new(TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE])),
        "sink".to_string(),
    );
    pipeline.connect_nodes(
        table_info.node.clone(),
        table_info.port,
        "sink".to_string(),
        DEFAULT_PORT_HANDLE,
    );

    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.into_dag().unwrap();

    let now = std::time::Instant::now();

    let runtime_clone = runtime.clone();
    let handle = runtime.block_on(async move {
        DagExecutor::new(dag, Default::default())
            .await
            .unwrap()
            .start(pending::<()>(), Default::default(), runtime_clone)
            .await
            .unwrap()
    });
    handle.join().unwrap();

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}

#[test]
fn streaming_membership_builds_executable_dag_with_original_output_schema() {
    let runtime = create_test_runtime();
    let mut pipeline = AppPipeline::new_with_default_flags();
    let context = statement_to_pipeline(
        "SELECT * INTO results FROM users WHERE CustomerID IN (SELECT CustomerID FROM allowed)",
        &mut pipeline,
        None,
        vec![],
        runtime.clone(),
    )
    .unwrap();
    let output = &context.output_tables_map["results"];
    pipeline.add_sink(Box::new(MembershipSchemaSinkFactory), "sink".into());
    pipeline.connect_nodes(
        output.node.clone(),
        output.port,
        "sink".into(),
        DEFAULT_PORT_HANDLE,
    );
    let mut sources = AppSourceManager::new();
    sources
        .add(
            Box::new(TestSourceFactory::new(vec![0, 1])),
            AppSourceMappings::new(
                "test".into(),
                HashMap::from([("users".into(), 0), ("allowed".into(), 1)]),
            ),
        )
        .unwrap();
    let mut app = App::new(sources);
    app.add_pipeline(pipeline);
    let dag = app.into_dag().unwrap();
    runtime
        .block_on(DagExecutor::new(dag, Default::default()))
        .unwrap();
}

#[derive(Debug)]
struct MembershipSchemaSinkFactory;

#[async_trait]
impl SinkFactory for MembershipSchemaSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }
    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        "membership".into()
    }
    async fn build(
        &self,
        schemas: HashMap<PortHandle, Schema>,
        _hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        self.prepare(schemas)?;
        Ok(Box::new(TestSink {}))
    }
    fn prepare(&self, schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        let schema = &schemas[&DEFAULT_PORT_HANDLE];
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["CustomerID", "Country", "Spending", "timestamp"]
        );
        assert!(schema.primary_index.is_empty());
        Ok(())
    }
    fn type_name(&self) -> String {
        "membership-schema-test".into()
    }
}

fn membership_test_dag(
    sql: &str,
    include_local: bool,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
) -> Result<dozer_core::Dag, dozer_core::errors::ExecutionError> {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let context = statement_to_pipeline(sql, &mut pipeline, None, vec![], runtime).unwrap();
    let output = &context.output_tables_map["results"];
    pipeline.add_sink(Box::new(MembershipSchemaSinkFactory), "sink".into());
    pipeline.connect_nodes(
        output.node.clone(),
        output.port,
        "sink".into(),
        DEFAULT_PORT_HANDLE,
    );
    let mut mappings = HashMap::from([("users".into(), 0), ("allowed".into(), 1)]);
    if include_local {
        mappings.insert("local".into(), 2);
    }
    let mut sources = AppSourceManager::new();
    sources
        .add(
            Box::new(TestSourceFactory::new(mappings.values().copied().collect())),
            AppSourceMappings::new("test".into(), mappings),
        )
        .unwrap();
    let mut app = App::new(sources);
    app.add_pipeline(pipeline);
    app.into_dag()
}

#[test]
fn streaming_membership_cte_scope_is_validated_by_executable_dag() {
    let runtime = create_test_runtime();
    let sql = "SELECT * INTO results FROM users WHERE CustomerID IN (WITH local AS (SELECT CustomerID FROM allowed) SELECT CustomerID FROM local) AND CustomerID IN (SELECT CustomerID FROM local)";
    let error = membership_test_dag(sql, false, runtime.clone()).unwrap_err();
    assert!(
        matches!(error, dozer_core::errors::ExecutionError::InvalidSourceIdentifier(ref name) if name == "local")
    );
    let dag = membership_test_dag(sql, true, runtime.clone()).unwrap();
    runtime
        .block_on(DagExecutor::new(dag, Default::default()))
        .unwrap();
}

#[test]
fn streaming_membership_rejects_correlated_and_unsupported_scalar_forms() {
    let runtime = create_test_runtime();
    for sql in [
        "SELECT * INTO results FROM users WHERE CustomerID IN (SELECT CustomerID FROM allowed WHERE allowed.CustomerID = users.CustomerID)",
        "SELECT * INTO results FROM users WHERE CASE WHEN CustomerID IN (SELECT CustomerID FROM allowed) THEN true ELSE false END",
    ] {
        let dag = membership_test_dag(sql, false, runtime.clone()).unwrap();
        let result = runtime.block_on(DagExecutor::new(dag, Default::default()));
        assert!(result.is_err(), "unsupported SQL must not silently produce wrong output: {sql}");
    }
}

#[test]
fn streaming_membership_preserves_valid_alias_qualified_queries() {
    let runtime = create_test_runtime();
    let sql = "SELECT u.* INTO results FROM users u WHERE u.CustomerID IN (SELECT a.CustomerID FROM allowed a)";
    let dag = membership_test_dag(sql, false, runtime.clone()).unwrap();
    runtime
        .block_on(DagExecutor::new(dag, Default::default()))
        .unwrap();
}
