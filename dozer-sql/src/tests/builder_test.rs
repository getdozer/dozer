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
use std::sync::{Arc, Mutex};

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
    recorded: Option<Arc<Mutex<Vec<TableOperation>>>>,
}

impl TestSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>) -> Self {
        Self {
            input_ports,
            recorded: None,
        }
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
        Ok(Box::new(TestSink {
            recorded: self.recorded.clone(),
        }))
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    fn type_name(&self) -> String {
        "test".to_string()
    }
}

#[derive(Debug)]
pub struct TestSink {
    recorded: Option<Arc<Mutex<Vec<TableOperation>>>>,
}

impl Sink for TestSink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        println!("Sink: {:?}", op);
        if let Some(recorded) = &self.recorded {
            recorded.lock().unwrap().push(op);
        }
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
fn test_in_subquery_builds_streaming_dag() {
    let sql = "SELECT outer_users.CustomerID FROM users outer_users \
         WHERE outer_users.CustomerID IN (SELECT inner_users.CustomerID FROM users inner_users)";
    build_subquery_dag(sql).unwrap();
}

fn build_subquery_dag(sql: &str) -> Result<(), BoxedError> {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let runtime = create_test_runtime();
    let context = statement_to_pipeline(
        sql,
        &mut pipeline,
        Some("results".to_string()),
        vec![],
        runtime.clone(),
    )?;
    let output = context.output_tables_map.get("results").unwrap();
    let mut sources = AppSourceManager::new();
    sources
        .add(
            Box::new(TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE])),
            AppSourceMappings::new(
                "mem".to_string(),
                vec![("users".to_string(), DEFAULT_PORT_HANDLE)]
                    .into_iter()
                    .collect(),
            ),
        )
        .unwrap();
    let recorded = Arc::new(Mutex::new(Vec::new()));
    pipeline.add_sink(
        Box::new(TestSinkFactory {
            input_ports: vec![DEFAULT_PORT_HANDLE],
            recorded: Some(recorded.clone()),
        }),
        "sink".to_string(),
    );
    pipeline.connect_nodes(
        output.node.clone(),
        output.port,
        "sink".to_string(),
        DEFAULT_PORT_HANDLE,
    );
    let mut app = App::new(sources);
    app.add_pipeline(pipeline);
    let dag = app.into_dag()?;
    let executor = runtime.block_on(DagExecutor::new(dag, Default::default()))?;
    let handle =
        runtime.block_on(executor.start(pending::<()>(), Default::default(), runtime.clone()))?;
    handle.join()?;
    let mut count = 0;
    for operation in recorded.lock().unwrap().iter() {
        match &operation.op {
            Operation::Insert { new } => {
                assert_eq!(new.values, vec![Field::Int(0)]);
                count += 1;
            }
            Operation::Delete { old } => {
                assert_eq!(old.values, vec![Field::Int(0)]);
                count -= 1;
            }
            Operation::Update { new, .. } => assert_eq!(new.values, vec![Field::Int(0)]),
            Operation::BatchInsert { new } => count += new.len() as i32,
        }
    }
    assert_eq!(
        count, 10,
        "each outer duplicate must appear once, without internal columns"
    );
    Ok(())
}

#[test]
fn test_in_subquery_multiple_predicates_and_nested_queries() {
    for predicate in [
        "CustomerID IN (SELECT CustomerID FROM users) AND CustomerID IN (SELECT CustomerID FROM users)",
        "CustomerID NOT IN (SELECT CustomerID FROM users WHERE CustomerID = 999)",
        "NOT (CustomerID NOT IN (SELECT CustomerID FROM users))",
        "CustomerID IN (SELECT CustomerID FROM users WHERE CustomerID IN (SELECT CustomerID FROM users))",
        "CustomerID IN (WITH selected AS (SELECT CustomerID FROM users) SELECT CustomerID FROM selected)",
    ] {
        build_subquery_dag(&format!("SELECT CustomerID FROM users WHERE {predicate}")).unwrap();
    }
}

#[test]
fn test_in_subquery_rejects_multiple_columns_and_correlation() {
    for sql in [
        "SELECT CustomerID FROM users WHERE CustomerID IN (SELECT CustomerID, Country FROM users)",
        "SELECT outer_users.CustomerID FROM users outer_users WHERE outer_users.CustomerID IN (SELECT inner_users.CustomerID FROM users inner_users WHERE inner_users.CustomerID = outer_users.CustomerID)",
    ] {
        assert!(build_subquery_dag(sql).is_err());
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
