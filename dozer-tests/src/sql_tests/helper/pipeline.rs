use ahash::AHasher;
use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::checkpoint::OptionCheckpoint;
use dozer_core::dozer_log::storage::Queue;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
    SourceState,
};

use dozer_core::{Dag, DEFAULT_PORT_HANDLE};
use dozer_recordstore::{ProcessorRecordStore, StoreRecord};

use dozer_core::executor::DagExecutor;

use crossbeam::channel::{Receiver, Sender};
use dozer_sql::builder::statement_to_pipeline;

use dozer_types::errors::internal::BoxedError;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::types::{Operation, Record, Schema, SourceDefinition};
use std::collections::HashMap;
use tempdir::TempDir;
use tokio::runtime::Runtime;

use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use std::time::Duration;

#[derive(Debug)]
pub(crate) struct TestSourceFactory {
    schemas: HashMap<u16, Schema>,
    name_to_port: HashMap<String, u16>,
    receiver: Receiver<Option<(String, Operation)>>,
}

impl TestSourceFactory {
    pub fn new(
        schemas: HashMap<u16, Schema>,
        name_to_port: HashMap<String, u16>,
        receiver: Receiver<Option<(String, Operation)>>,
    ) -> Self {
        Self {
            schemas,
            name_to_port,
            receiver,
        }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
        let mut schema = self
            .schemas
            .get(port)
            .expect("schemas should have been initialized with enumerated index")
            .to_owned();

        let table_name = self
            .name_to_port
            .iter()
            .find(|(_, p)| **p == *port)
            .unwrap()
            .0
            .clone();
        // Add source information to the schema.
        let mut fields = vec![];
        for field in schema.fields {
            let mut f = field.clone();
            f.source = SourceDefinition::Table {
                connection: "test_connection".to_string(),
                name: table_name.clone(),
            };
            fields.push(f);
        }
        schema.fields = fields;

        Ok(schema)
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        self.name_to_port
            .iter()
            .find(|(_, p)| **p == *port)
            .unwrap()
            .0
            .clone()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.schemas
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                OutputPortDef::new(idx as u16, OutputPortType::StatefulWithPrimaryKeyLookup)
            })
            .collect()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(TestSource {
            name_to_port: self.name_to_port.to_owned(),
            receiver: self.receiver.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct TestSource {
    name_to_port: HashMap<String, u16>,
    receiver: Receiver<Option<(String, Operation)>>,
}

impl Source for TestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        while let Ok(Some((schema_name, op))) = self.receiver.recv() {
            let port = self.name_to_port.get(&schema_name).expect("port not found");
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op,
                    id: None,
                },
                *port,
            )
            .unwrap();
        }
        thread::sleep(Duration::from_millis(200));

        //self.running
        //    .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug)]
pub struct SchemaHolder {
    pub schema: Option<Schema>,
}

#[derive(Debug)]
pub struct TestSinkFactory {
    output: Arc<Mutex<HashMap<Vec<u8>, Vec<Record>>>>,
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(output: Arc<Mutex<HashMap<Vec<u8>, Vec<Record>>>>) -> Self {
        Self {
            output,
            input_ports: vec![DEFAULT_PORT_HANDLE],
        }
    }
}

impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(TestSink::new(self.output.to_owned())))
    }
}

#[derive(Debug)]
pub struct TestSink {
    output: Arc<Mutex<HashMap<Vec<u8>, Vec<Record>>>>,
}

impl TestSink {
    pub fn new(output: Arc<Mutex<HashMap<Vec<u8>, Vec<Record>>>>) -> Self {
        Self { output }
    }

    fn update_result(
        &mut self,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        let mut records_map = self.output.lock().expect("Unable to lock the result map");
        match op {
            ProcessorOperation::Insert { new } => {
                let new = record_store.load_record(&new)?;
                let records_item = records_map.get_mut(&get_key(&new));

                if let Some(records) = records_item {
                    records.push(new);
                } else {
                    records_map.insert(get_key(&new), vec![new]);
                }
            }
            ProcessorOperation::Delete { ref old } => {
                let old = record_store.load_record(old)?;
                if let Some(map_records) = records_map.get_mut(&get_key(&old)) {
                    if let Some(index) = map_records.iter().position(|x| *x == old) {
                        map_records.remove(index);
                    }
                }
            }
            ProcessorOperation::Update { ref old, new } => {
                let new = record_store.load_record(&new)?;
                let old = record_store.load_record(old)?;
                if let Some(map_records) = records_map.get_mut(&get_key(&old)) {
                    if let Some(index) = map_records.iter().position(|x| *x == old) {
                        map_records.remove(index);
                    }
                }

                let records_item = records_map.get_mut(&get_key(&new));

                if let Some(records) = records_item {
                    records.push(new);
                } else {
                    records_map.insert(get_key(&new), vec![new]);
                }
            }
        }
        Ok(())
    }
}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        self.update_result(record_store, op)
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn persist(&mut self, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}

pub struct TestPipeline {
    pub schema: Schema,
    pub dag: Dag,
    pub used_schemas: Vec<String>,
    pub sender: Sender<Option<(String, Operation)>>,
    pub ops: Vec<(String, Operation)>,
    pub result: Arc<Mutex<HashMap<Vec<u8>, Vec<Record>>>>,
}

impl TestPipeline {
    pub fn new(
        sql: String,
        schemas: HashMap<String, Schema>,
        ops: Vec<(String, Operation)>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self::build_pipeline(sql, schemas, ops, runtime).unwrap()
    }

    pub fn build_pipeline(
        sql: String,
        schemas: HashMap<String, Schema>,
        ops: Vec<(String, Operation)>,
        runtime: Arc<Runtime>,
    ) -> Result<TestPipeline, ExecutionError> {
        let mut pipeline = AppPipeline::new_with_default_flags();

        let transform_response = statement_to_pipeline(
            &sql,
            &mut pipeline,
            Some("results".to_string()),
            vec![],
            runtime,
        )
        .unwrap();

        let output_table = transform_response.output_tables_map.get("results").unwrap();
        let (sender, receiver) = crossbeam::channel::bounded::<Option<(String, Operation)>>(1000);

        let mut port_to_schemas = HashMap::new();
        let mut mappings = HashMap::new();

        for (port, name) in transform_response.used_sources.iter().enumerate() {
            if !mappings.contains_key(name) {
                mappings.insert(name.to_string(), port as u16);
                port_to_schemas.insert(port as u16, schemas.get(name).unwrap().clone());
            }
        }

        let mut asm = AppSourceManager::new();

        asm.add(
            Box::new(TestSourceFactory::new(
                port_to_schemas,
                mappings.clone(),
                receiver,
            )),
            AppSourceMappings::new("test_connection".to_string(), mappings),
        )
        .unwrap();

        let output = Arc::new(Mutex::new(HashMap::new()));
        pipeline.add_sink(Box::new(TestSinkFactory::new(output.clone())), "sink", None);

        pipeline.connect_nodes(
            &output_table.node,
            output_table.port,
            "sink",
            DEFAULT_PORT_HANDLE,
        );
        let used_schemas = pipeline.get_entry_points_sources_names();
        let mut app = App::new(asm);
        app.add_pipeline(pipeline);

        let dag = app.into_dag()?;

        // dag.print_dot();

        Ok(TestPipeline {
            schema: Schema::default(),
            dag,
            used_schemas,

            sender,
            ops,
            result: output,
        })
    }

    pub async fn run(self) -> Result<Vec<Vec<String>>, ExecutionError> {
        let temp_dir = TempDir::new("test")
            .map_err(|e| ExecutionError::FileSystemError("tempdir".into(), e))?;
        let checkpoint_dir = temp_dir
            .path()
            .to_str()
            .expect("Path should always be utf8")
            .to_string();
        let checkpoint = OptionCheckpoint::new(checkpoint_dir, Default::default()).await?;
        let executor = DagExecutor::new(self.dag, checkpoint, Default::default()).await?;
        let join_handle = executor
            .start(Arc::new(AtomicBool::new(true)), Default::default())
            .await?;

        for (schema_name, op) in &self.ops {
            if self.used_schemas.contains(&schema_name.to_string()) {
                self.sender
                    .send(Some((schema_name.to_string(), op.clone())))
                    .unwrap();
            }
        }
        self.sender.send(None).unwrap();

        join_handle.join()?;

        let mut output = vec![];

        let result_map = self.result.lock().expect("Unable to lock the result map");
        // iterate over all keys and the key's vector.
        for (_, values) in result_map.iter() {
            for value in values {
                let mut row = vec![];
                for field in &value.values {
                    let value = field.to_string();
                    row.push(value);
                }
                output.push(row);
            }
        }

        Ok(output)
    }
}

fn get_key(record: &Record) -> Vec<u8> {
    let mut hasher = AHasher::default();
    record.values.hash(&mut hasher);
    let key = hasher.finish();
    key.to_be_bytes().to_vec()
}
