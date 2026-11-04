//! Actual in-memory ingestion, SQL planning, DAG execution and collecting sink.
//! Each input operation is followed by an executor commit barrier. No sleeps or
//! assumptions about scheduling between the outer and inner graph branches.
use std::{
    collections::HashMap,
    future::pending,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use dozer_core::{
    app::{App, AppPipeline},
    appsource::{AppSourceManager, AppSourceMappings},
    epoch::Epoch,
    event::EventHub,
    executor::DagExecutor,
    node::{OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory},
};
use dozer_sql::builder::statement_to_pipeline;
use dozer_types::{
    errors::internal::BoxedError,
    models::ingestion_types::{IngestionMessage, TransactionInfo},
    node::OpIdentifier,
    tonic::async_trait,
    types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};
use tokio::sync::{mpsc::Sender, Notify};

#[derive(Clone, Debug)]
pub struct Step {
    pub label: &'static str,
    pub port: PortHandle,
    pub operation: Operation,
    pub expected: Vec<Record>,
    pub emitted: Option<Vec<Operation>>,
}

pub fn row(key: i64) -> Record {
    Record::new(vec![Field::Int(key)])
}
pub fn pair(key: i64, payload: i64) -> Record {
    Record::new(vec![Field::Int(key), Field::Int(payload)])
}
pub fn null() -> Record {
    Record::new(vec![Field::Null])
}
pub fn insert(new: Record) -> Operation {
    Operation::Insert { new }
}
pub fn delete(old: Record) -> Operation {
    Operation::Delete { old }
}
pub fn update(old: Record, new: Record) -> Operation {
    Operation::Update { old, new }
}
pub fn step(
    label: &'static str,
    port: PortHandle,
    operation: Operation,
    expected: Vec<Record>,
) -> Step {
    Step {
        label,
        port,
        operation,
        expected,
        emitted: None,
    }
}
pub fn schema(fields: &[(&str, FieldType)]) -> Schema {
    Schema {
        fields: fields
            .iter()
            .map(|(name, typ)| {
                FieldDefinition::new((*name).into(), *typ, true, SourceDefinition::Dynamic)
            })
            .collect(),
        primary_index: vec![],
    }
}
pub fn tables() -> Vec<(String, Schema)> {
    ["outer_rows", "members", "others", "more"]
        .into_iter()
        .map(|name| {
            (
                name.into(),
                schema(&[("key", FieldType::Int), ("payload", FieldType::Int)]),
            )
        })
        .collect()
}

#[derive(Debug, Default)]
struct State {
    completed: AtomicUsize,
    notify: Notify,
    schema: Mutex<Option<Schema>>,
    snapshots: Mutex<Vec<Vec<Record>>>,
}

#[derive(Debug)]
struct MemorySource {
    tables: Vec<(String, Schema)>,
    steps: Vec<Step>,
    state: Arc<State>,
    print: bool,
}
impl SourceFactory for MemorySource {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        (0..self.tables.len())
            .map(|port| OutputPortDef::new(port as PortHandle, OutputPortType::Stateless))
            .collect()
    }
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(self.tables[usize::from(*port)].1.clone())
    }
    fn get_output_port_name(&self, port: &PortHandle) -> String {
        self.tables[usize::from(*port)].0.clone()
    }
    fn build(
        &self,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
        _: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(Self {
            tables: self.tables.clone(),
            steps: self.steps.clone(),
            state: self.state.clone(),
            print: self.print,
        }))
    }
}
#[async_trait]
impl Source for MemorySource {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }
    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        _: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        for (index, step) in self.steps.iter().enumerate() {
            // Only the terminal demo is paced for readability. Tests use print=false
            // and synchronize solely on the real executor commit acknowledgement.
            if self.print {
                tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
            }
            if self.print {
                println!(
                    "\nSTEP {}: {}\nINPUT {}: {:?}",
                    index + 1,
                    step.label,
                    self.tables[usize::from(step.port)].0,
                    step.operation
                );
            }
            sender
                .send((
                    step.port,
                    IngestionMessage::OperationEvent {
                        table_index: usize::from(step.port),
                        op: step.operation.clone(),
                        id: Some(OpIdentifier::new(index as u64 + 1, 0)),
                    },
                ))
                .await?;
            sender
                .send((
                    step.port,
                    IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                        id: None,
                        source_time: None,
                    }),
                ))
                .await?;
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let notified = self.state.notify.notified();
                    if self.state.completed.load(Ordering::Acquire) > index {
                        break;
                    }
                    notified.await;
                }
            })
            .await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MemorySink {
    steps: Vec<Step>,
    state: Arc<State>,
    rows: Vec<Record>,
    operations: Vec<Operation>,
    print: bool,
}
#[async_trait]
impl SinkFactory for MemorySink {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![0]
    }
    fn get_input_port_name(&self, _: &PortHandle) -> String {
        "result".into()
    }
    fn type_name(&self) -> String {
        "memory-result".into()
    }
    fn prepare(&self, schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        *self.state.schema.lock().unwrap() = Some(schemas[&0].clone());
        Ok(())
    }
    async fn build(
        &self,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(Self {
            steps: self.steps.clone(),
            state: self.state.clone(),
            rows: vec![],
            operations: vec![],
            print: self.print,
        }))
    }
}
fn multiset(rows: &[Record]) -> HashMap<Record, usize> {
    let mut counts = HashMap::new();
    for row in rows {
        *counts.entry(row.clone()).or_default() += 1;
    }
    counts
}
impl MemorySink {
    fn remove(&mut self, record: &Record) {
        let index = self
            .rows
            .iter()
            .position(|row| row == record)
            .expect("engine retracted an absent output row");
        self.rows.remove(index);
    }
}
impl Sink for MemorySink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        if self.print {
            println!("OUTPUT: {:?}", op.op);
        }
        self.operations.push(op.op.clone());
        match op.op {
            Operation::Insert { new } => self.rows.push(new),
            Operation::Delete { old } => self.remove(&old),
            Operation::Update { old, new } => {
                self.remove(&old);
                self.rows.push(new);
            }
            Operation::BatchInsert { new } => self.rows.extend(new),
        }
        Ok(())
    }
    fn commit(&mut self, epoch: &Epoch) -> Result<(), BoxedError> {
        let index = self.state.completed.load(Ordering::Acquire);
        assert_eq!(epoch.common_info.id as usize, index);
        let step = &self.steps[index];
        assert_eq!(
            multiset(&self.rows),
            multiset(&step.expected),
            "step {}: {}",
            index + 1,
            step.label
        );
        if let Some(expected) = &step.emitted {
            let mut actual = self.operations.clone();
            for operation in expected {
                let position = actual
                    .iter()
                    .position(|op| op == operation)
                    .expect("missing expected output operation");
                actual.remove(position);
            }
            assert!(
                actual.is_empty(),
                "unexpected extra output at {}: {actual:?}",
                step.label
            );
        }
        if self.print {
            println!(
                "COMMITTED RESULT: {:?}\nASSERTIONS: PASS ({} emitted operations)",
                self.rows,
                self.operations.len()
            );
        }
        self.operations.clear();
        self.state.snapshots.lock().unwrap().push(self.rows.clone());
        self.state.completed.store(index + 1, Ordering::Release);
        self.state.notify.notify_one();
        Ok(())
    }
    fn on_source_snapshotting_started(&mut self, _: String) -> Result<(), BoxedError> {
        Ok(())
    }
    fn on_source_snapshotting_done(
        &mut self,
        _: String,
        _: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
    fn set_source_state(&mut self, _: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }
    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }
    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}

/// With `execute=false`, exercise all real schema/factory construction without
/// starting ingestion. Tests can assert unsupported SQL fails at this boundary.
pub fn run(
    sql: &str,
    tables: Vec<(String, Schema)>,
    steps: Vec<Step>,
    execute: bool,
    print: bool,
) -> Result<Schema, BoxedError> {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?,
    );
    let state = Arc::new(State::default());
    let mut pipeline = AppPipeline::new_with_default_flags();
    let context = statement_to_pipeline(
        sql,
        &mut pipeline,
        Some("results".into()),
        vec![],
        runtime.clone(),
    )?;
    let output = context
        .output_tables_map
        .get("results")
        .expect("SQL output");
    pipeline.add_sink(
        Box::new(MemorySink {
            steps: steps.clone(),
            state: state.clone(),
            rows: vec![],
            operations: vec![],
            print,
        }),
        "sink".into(),
    );
    pipeline.connect_nodes(output.node.clone(), output.port, "sink".into(), 0);
    let mappings = tables
        .iter()
        .enumerate()
        .map(|(port, (name, _))| (name.clone(), port as PortHandle))
        .collect();
    let mut sources = AppSourceManager::new();
    sources.add(
        Box::new(MemorySource {
            tables,
            steps: steps.clone(),
            state: state.clone(),
            print,
        }),
        AppSourceMappings::new("memory".into(), mappings),
    )?;
    let mut app = App::new(sources);
    app.add_pipeline(pipeline);
    let dag = app.into_dag()?;
    let executor = runtime.block_on(DagExecutor::new(dag, Default::default()))?;
    if execute {
        if print {
            println!(
                "SQL: {sql}\nENGINE: actual SQL -> pipeline -> DAG -> source ingestion -> sink"
            );
        }
        let handle = runtime.block_on(executor.start(
            pending::<()>(),
            Default::default(),
            runtime.clone(),
        ))?;
        handle.join()?;
        assert_eq!(state.completed.load(Ordering::Acquire), steps.len());
    }
    let schema = state.schema.lock().unwrap().clone().expect("sink schema");
    Ok(schema)
}
