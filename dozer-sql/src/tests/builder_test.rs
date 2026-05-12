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

#[derive(Debug)]
pub struct ScriptedSourceFactory {
    output_ports: Vec<PortHandle>,
    operations: Vec<(PortHandle, Operation)>,
}

impl ScriptedSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>, operations: Vec<(PortHandle, Operation)>) -> Self {
        Self {
            output_ports,
            operations,
        }
    }
}

impl SourceFactory for ScriptedSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect()
    }

    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
        let table_name = if *port == 1 { "allowed" } else { "users" };
        Ok(Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("CustomerID"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Table {
                        connection: "mem".to_string(),
                        name: table_name.to_string(),
                    },
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Country"),
                    FieldType::String,
                    false,
                    SourceDefinition::Table {
                        connection: "mem".to_string(),
                        name: table_name.to_string(),
                    },
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Spending"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Table {
                        connection: "mem".to_string(),
                        name: table_name.to_string(),
                    },
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("timestamp"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Table {
                        connection: "mem".to_string(),
                        name: table_name.to_string(),
                    },
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
        Ok(Box::new(ScriptedSource {
            operations: self.operations.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct ScriptedSource {
    operations: Vec<(PortHandle, Operation)>,
}

#[async_trait]
impl Source for ScriptedSource {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        for (port, op) in self.operations.clone() {
            sender
                .send((
                    port,
                    IngestionMessage::OperationEvent {
                        table_index: port as usize,
                        op,
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
pub struct CollectingSinkFactory {
    operations: Arc<Mutex<Vec<TableOperation>>>,
}

impl CollectingSinkFactory {
    pub fn new(operations: Arc<Mutex<Vec<TableOperation>>>) -> Self {
        Self { operations }
    }
}

#[async_trait]
impl SinkFactory for CollectingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        "test".to_string()
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(CollectingSink {
            operations: self.operations.clone(),
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
pub struct CollectingSink {
    operations: Arc<Mutex<Vec<TableOperation>>>,
}

impl Sink for CollectingSink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        self.operations.lock().unwrap().push(op);
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

fn scripted_record(customer_id: i64, country: &str, spending: f64, timestamp: &str) -> Record {
    Record::new(vec![
        Field::Int(customer_id),
        Field::String(country.to_string()),
        Field::Float(OrderedFloat(spending)),
        Field::Timestamp(DateTime::parse_from_rfc3339(timestamp).unwrap()),
    ])
}

fn scripted_insert(record: Record) -> Operation {
    Operation::Insert { new: record }
}

fn execute_scripted_query(sql: &str, operations: Vec<(PortHandle, Operation)>) -> Vec<Vec<Field>> {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let runtime = create_test_runtime();
    let context = statement_to_pipeline(sql, &mut pipeline, None, vec![], runtime.clone()).unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();
    let output_operations = Arc::new(Mutex::new(vec![]));

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(ScriptedSourceFactory::new(
            vec![DEFAULT_PORT_HANDLE, 1],
            operations,
        )),
        AppSourceMappings::new(
            "mem".to_string(),
            vec![
                ("users".to_string(), DEFAULT_PORT_HANDLE),
                ("allowed".to_string(), 1),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();

    pipeline.add_sink(
        Box::new(CollectingSinkFactory::new(output_operations.clone())),
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

    let rows = output_operations
        .lock()
        .unwrap()
        .iter()
        .filter_map(|op| match &op.op {
            Operation::Insert { new } => Some(new.values.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    rows
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
fn test_in_subquery_filters_stream_with_inner_select_membership() {
    let rows = execute_scripted_query(
        "SELECT users.CustomerID \
         INTO results \
         FROM users \
         WHERE users.CustomerID IN (SELECT allowed.CustomerID FROM allowed)",
        vec![
            (
                1,
                scripted_insert(scripted_record(7, "Allowed", 0.0, "2020-01-01T00:00:00Z")),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(7, "Italy", 5.5, "2020-01-01T00:13:00Z")),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(8, "France", 7.0, "2020-01-01T00:14:00Z")),
            ),
        ],
    );

    assert_eq!(rows, vec![vec![Field::Int(7)]]);
}

#[test]
fn test_in_subquery_emits_when_inner_membership_arrives_later() {
    let rows = execute_scripted_query(
        "SELECT users.CustomerID \
         INTO results \
         FROM users \
         WHERE users.CustomerID IN (SELECT allowed.CustomerID FROM allowed)",
        vec![
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(7, "Italy", 5.5, "2020-01-01T00:13:00Z")),
            ),
            (
                1,
                scripted_insert(scripted_record(7, "Allowed", 0.0, "2020-01-01T00:14:00Z")),
            ),
        ],
    );

    assert_eq!(rows, vec![vec![Field::Int(7)]]);
}

#[test]
fn test_in_subquery_qualifies_unqualified_outer_identifier() {
    let rows = execute_scripted_query(
        "SELECT users.CustomerID \
         INTO results \
         FROM users \
         WHERE CustomerID IN (SELECT allowed.CustomerID FROM allowed)",
        vec![
            (
                1,
                scripted_insert(scripted_record(7, "Allowed", 0.0, "2020-01-01T00:00:00Z")),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(7, "Italy", 5.5, "2020-01-01T00:13:00Z")),
            ),
        ],
    );

    assert_eq!(rows, vec![vec![Field::Int(7)]]);
}

#[test]
fn test_in_subquery_keeps_additional_where_predicates() {
    let rows = execute_scripted_query(
        "SELECT users.CustomerID \
         INTO results \
         FROM users \
         WHERE users.Spending > 6 \
         AND users.CustomerID IN (SELECT allowed.CustomerID FROM allowed)",
        vec![
            (
                1,
                scripted_insert(scripted_record(7, "Allowed", 0.0, "2020-01-01T00:00:00Z")),
            ),
            (
                1,
                scripted_insert(scripted_record(8, "Allowed", 0.0, "2020-01-01T00:00:01Z")),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(7, "Italy", 5.5, "2020-01-01T00:13:00Z")),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(8, "France", 7.0, "2020-01-01T00:14:00Z")),
            ),
        ],
    );

    assert_eq!(rows, vec![vec![Field::Int(8)]]);
}

#[test]
fn test_in_subquery_uses_membership_semantics_for_duplicate_inner_rows() {
    let rows = execute_scripted_query(
        "SELECT users.CustomerID \
         INTO results \
         FROM users \
         WHERE users.CustomerID IN (SELECT allowed.CustomerID FROM allowed)",
        vec![
            (
                1,
                scripted_insert(scripted_record(7, "Allowed", 0.0, "2020-01-01T00:00:00Z")),
            ),
            (
                1,
                scripted_insert(scripted_record(
                    7,
                    "Allowed duplicate",
                    1.0,
                    "2020-01-01T00:00:01Z",
                )),
            ),
            (
                DEFAULT_PORT_HANDLE,
                scripted_insert(scripted_record(7, "Italy", 5.5, "2020-01-01T00:13:00Z")),
            ),
        ],
    );

    assert_eq!(rows, vec![vec![Field::Int(7)]]);
}
